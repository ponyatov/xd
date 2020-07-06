
import os, sys, re

MODULE = re.findall(r'([a-z]+)\.py$', sys.argv[0])[0]

## logging

import logging
logging.basicConfig(
    filename=re.sub(r'.py$', r'.log', sys.argv[0]), filemode='w',
    level=logging.DEBUG)
logging.info(('MODULE', MODULE))

## storage

import threading, queue, pymysql, json

DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PSWD = os.environ['DB_PSWD']
DB_BASE = os.environ['DB_BASE']

db = pymysql.connect(
    host=DB_HOST, db=DB_BASE,
    user=DB_USER, password=DB_PSWD,
    charset='utf8mb4',
    # cursorclass=DictCursor
)

try:
    db.cursor().execute(
        (
            "create table %s (" +
            "k char(8) primary key, " +
            "type varchar(32), " +
            "val text, " +
            "ref int, " +
            "v json);"
        ) % MODULE)
    db.cursor().execute(
        'alter table %s add index idx_%s_k(k);' % (MODULE, MODULE))
    db.cursor().execute(
        'alter table %s add index idx_%s_type(type);' % (MODULE, MODULE))
    db.cursor().execute(
        'alter table %s add index idx_%s_val(val(32));' % (MODULE, MODULE))
    db.cursor().execute(
        'alter table %s add index idx_%s_ref(ref);' % (MODULE, MODULE))
    db.commit()
    sys.exit(0)
except:
    pass

storage = queue.Queue(maxsize=0x111)

def storage_daemon():
    cur = db.cursor()
    while True:
        try:
            item = storage.get(timeout=1)
        except queue.Empty:
            item = None
        if item == 'BYE':
            break
        elif item:
        #    try:
                js = item.json()
                try:
                    sql = "insert into %s(k,type,val,ref,v) values ('%s','%s','%s',%s,'%s');" % (
                        MODULE, item.gid, item._type(), item.val, item.ref, js)
                    cur.execute(sql)
                    logging.debug(sql)
                except pymysql.err.IntegrityError:
                    sql = "update %s set type='%s',val='%s',ref=%s,v='%s' where k='%s';" % (
                        MODULE, item._type(), item.val, item.ref, js, item.gid)
                    cur.execute(sql)
                    logging.debug(sql)
                db.commit()
            # except:
            #     pass


storage_thread = threading.Thread(target=storage_daemon)
storage_thread.start()

## Object graph

from xxhash import xxh32

class Object(object):
    def __init__(self, V):
        self.val = V
        self.slot = {}
        self.nest = []
        self.ref = 0
        self.sync()

    ## storage

    def sync(self):
        self.gid = '%.8x' % hash(self)
        storage.put(self)

    def __hash__(self):
        g = xxh32(self._type())
        g.update('%s' % self.val)
        return g.intdigest()

    def json(self):
        js = {"gid": self.gid, "ref": self.ref,
              "type": self._type(), "val": self.val}
        js['slot'] = {}
        for i in self.slot:
            js['slot'][i] = self.slot[i].gid
        js['nest'] = []
        for j in self.nest:
            js['nest'].append(j.gid)
        return json.dumps(js)

    ## dump

    def __repr__(self): return self.dump()

    def dump(self, done=None, depth=0, prefix='', test=False):
        # header
        tree = self._pad(depth) + self.head(prefix, test)
        # block cycles
        if not depth:
            done = []
        if self in done:
            return tree + ' _/'
        else:
            done.append(self)
        # slot{}s
        for i in sorted(self.slot.keys()):
            tree += self.slot[i].dump(done, depth + 1, '%s = ' % i, test)
        # nest[]ed
        idx = 0
        for j in self.nest:
            tree += j.dump(done, depth + 1, '%i: ' % idx, test)
            idx += 1
        # subtree
        return tree

    def head(self, prefix='', test=False):
        hdr = '%s<%s:%s>' % (prefix, self._type(), self._val())
        if not test:
            hdr += ' #%s @%s' % (self.gid, self.ref)
        return hdr

    def _pad(self, depth): return '\n' + '\t' * depth

    def _type(self): return self.__class__.__name__.lower()
    def _val(self): return '%s' % self.val

    ## operator

    def __getitem__(self, key):
        return self.slot[key]

    def __setitem__(self, key, that):
        self.slot[key] = that
        that.ref += 1
        self.sync()
        return self

    def __lshift__(self, that):
        return Object.__setitem__(self, that._type(), that)

    def __rshift__(self, that):
        return Object.__setitem__(self, that._val(), that)

    def __floordiv__(self, that):
        self.nest.append(that)
        that.ref += 1
        self.sync()
        return self

## error

class Error(Object, BaseException):
    pass

## primitive

class Primitive(Object):
    def eval(self, ctx): return self

class Symbol(Primitive):
    def eval(self, ctx):
        return ctx[self.val]

    def colon(self, that, ctx):
        lval = self.eval(ctx)
        rval = that
        return lval.colon(rval, ctx)

    def eq(self, that, ctx):
        lval = self
        rval = that.eval(ctx)
        ctx[lval.val] = rval
        return rval

    def at(self, that, ctx):
        lval = self.eval(ctx)
        rval = that
        return lval.at(rval, ctx)

class String(Primitive):
    pass

## active

class Active(Object):
    pass

class Op(Active):
    def eval(self, ctx):
        if len(self.nest) == 2:
            lval = self.nest[0]
            rval = self.nest[1]
        if self.val == ':':
            return lval.colon(rval, ctx)
        if self.val == '=':
            return lval.eq(rval, ctx)
        if self.val == '@':
            return lval.at(rval, ctx)
        if self.val == '`':
            return self.nest[0]
        raise Error((self))

    def at(self, that, ctx):
        lval = self.eval(ctx)
        rval = that
        return lval.at(rval, ctx)

class VM(Active):
    pass


vm = VM(MODULE)
vm // vm

## meta

class Meta(Object):
    pass

class Class(Meta):
    def __init__(self, C):
        Meta.__init__(self, C.__name__)
        self.cls = C

    def colon(self, that, ctx):
        return self.cls(that.val)

## I/O

class IO(Object):
    pass

class File(IO):
    pass

class PNG(File):
    pass

## debug

def BYE(ctx):
    storage.put(ctx)
    storage.put('BYE')
    storage_thread.join()
    db.commit()
    db.close()
    sys.exit(0)

## lexer


import ply.lex as lex

tokens = ['nl', 'symbol', 'string',
          'eq', 'at', 'colon', 'tick',
          'email', 'url']

t_ignore = ' \t\r'
t_ignore_comment = r'\#.*'

states = (('str', 'exclusive'),)

t_str_ignore = ''

def t_str(t):
    r'\''
    t.lexer.push_state('str')
    t.lexer.string = ''
def t_str_string(t):
    r'\''
    t.lexer.pop_state()
    t.value = String(t.lexer.string)
    return t
def t_str_any(t):
    r'.'
    t.lexer.string += t.value
def t_str_nl(t):
    r'\n'
    t.lexer.lineno += 1
    t.lexer.string += t.value

def t_nl(t):
    r'\n'
    t.lexer.lineno += 1
    return t

def t_eq(t):
    r'\='
    t.value = Op(t.value)
    return t
def t_at(t):
    r'\@'
    t.value = Op(t.value)
    return t
def t_colon(t):
    r'\:'
    t.value = Op(t.value)
    return t
def t_tick(t):
    r'\`'
    t.value = Op(t.value)
    return t

def t_email(t):
    r'[a-z]+@([a-z]+\.)+([a-z]+)'
    t.value = Email(t.value)
    return t
def t_url(t):
    r'https?://[^ \t\r\n]+'
    t.value = Url(t.value)
    return t
def t_symbol(t):
    r'[^ \t\r\n\#\=\:\']+'
    t.value = Symbol(t.value)
    return t

def t_ANY_error(t): raise SyntaxError(t)


lexer = lex.lex()

## parser

import ply.yacc as yacc

precedence = (
    ('right', 'eq'),
    ('left', 'at'),
    ('nonassoc', 'colon'),
)

def p_REPL_none(p):
    ' REPL : '
def p_REPL_nl(p):
    ' REPL : REPL nl '
def p_REPL_recur(p):
    ' REPL : REPL ex nl '
    logging.debug(p[2])
    logging.debug(p[2].eval(vm))
    logging.debug(vm)
    logging.debug('-' * 66)

def p_ex_symbol(p):
    ' ex : symbol '
    p[0] = p[1]
def p_ex_string(p):
    ' ex : string '
    p[0] = p[1]
def p_ex_email(p):
    ' ex : email '
    p[0] = p[1]
def p_ex_url(p):
    ' ex : url '
    p[0] = p[1]

def p_ex_colon(p):
    ' ex : ex colon ex '
    p[0] = p[2] // p[1] // p[3]
def p_ex_eq(p):
    ' ex : ex eq ex '
    p[0] = p[2] // p[1] // p[3]
def p_ex_at(p):
    ' ex : ex at ex '
    p[0] = p[2] // p[1] // p[3]

def p_ex_tick(p):
    ' ex : tick ex '
    p[0] = p[1] // p[2]

def p_error(p): raise SyntaxError(p)


parser = yacc.yacc(debug=False, write_tables=False)


## network/webinterface

class Net(IO):
    pass

class IP(Net):
    pass

class Port(Net):
    pass

class Email(Net, Primitive):
    pass
class Url(Net, Primitive):
    pass

class Web(Net):
    def __init__(self, V):
        Net.__init__(self, V)
        self['ip'] = IP(os.environ['IP'])
        self['port'] = Port(os.environ['PORT'])
        self['logo'] = PNG('logo.png')

    extra_files = []

    def eval(self, ctx):
        import flask
        app = flask.Flask(__name__)

        @app.route('/')
        def index(): return flask.render_template('index.html', ctx=ctx, web=self)

        @app.route('/<path:path>.css')
        def css(path): return app.send_static_file(path + '.css')

        @app.route('/<path:path>.png')
        def png(path): return app.send_static_file(path + '.png')

        @app.route('/<path:path>.js')
        def js(path): return app.send_static_file(path + '.js')

        app.run(host=self['ip'].val, port=self['port'].val,
                debug=True, extra_files=Web.extra_files)

    def apply(self, that, ctx):
        return self.eval(ctx)

    def at(self, that, ctx):
        return self.apply(that, ctx)


vm >> Class(Web)

## system init

def init():
    for srcfile in sys.argv[1:]:
        Web.extra_files.append(srcfile)
        with open(srcfile) as src:
            parser.parse(src.read())


if __name__ == '__main__':
    logging.info(sys.argv)
    logging.info(vm)
    init()
    BYE(vm)

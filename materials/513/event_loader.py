import sqlite3
def open_store(path):
    db=sqlite3.connect(path);db.execute('create table if not exists facts(key text primary key,value integer,version integer)');return db
def add_once(db,event):
    """Initial single submission, assumes caller supplies a valid first event."""
    key=event['key'];old=db.execute('select value,version from facts where key=?',(key,)).fetchone() or (0,0)
    with db:db.execute('insert or replace into facts values(?,?,?)',(key,old[0]+event['delta'],old[1]+1))
    return {'status':'applied','key':key,'value':old[0]+event['delta'],'version':old[1]+1}

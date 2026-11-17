import tempfile,json
from pathlib import Path
from event_loader import open_store,add_once
from deadletter_service import Delivery
with tempfile.TemporaryDirectory() as t:
    db=open_store(Path(t)/'facts.db');e=json.loads(Path(__file__).with_name('examples.json').read_text())[0];print(Delivery(lambda event:add_once(db,event)).send(e));print(db.execute('select * from facts').fetchall())

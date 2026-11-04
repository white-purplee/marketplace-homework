import os
import importlib
from alembic import context
from common.db import make_engine

config = context.config
metadata = importlib.import_module(config.get_main_option("service") + ".models").metadata
if context.is_offline_mode():
    context.configure(url=os.environ["DATABASE_URL"], target_metadata=metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = make_engine()
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()

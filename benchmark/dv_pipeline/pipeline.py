import pandas as pd
import sqlalchemy.orm as sa_orm
from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from sqlalchemy import Column, Integer, String

from benchmark.common import get_dbconn


class Base(sa_orm.DeclarativeBase):
    pass


class PerfSourceTable(Base):
    __tablename__ = "perf_source"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfTargetTable(Base):
    __tablename__ = "perf_target"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


def copy_transform(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


pipeline = Pipeline(
    [
        BatchTransform(
            copy_transform,
            inputs=[PerfSourceTable],
            outputs=[PerfTargetTable],
            transform_keys=["id"],
            chunk_size=100,
        )
    ]
)


def get_app() -> DatapipeApp:
    """Get the DatapipeApp instance, initializing if necessary."""
    dbconn = get_dbconn()
    app = DatapipeApp(
        ds=DataStore(dbconn),
        catalog=Catalog({}),
        pipeline=pipeline,
    )

    Base.metadata.create_all(dbconn.con)
    app.ds.meta_dbconn.sqla_metadata.create_all(dbconn.con)

    return app

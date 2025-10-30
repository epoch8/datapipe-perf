import pandas as pd
import sqlalchemy.orm as sa_orm
from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from sqlalchemy import Column, Integer, String

from benchmark.common import get_dbconn


class Base(sa_orm.DeclarativeBase):
    pass


class PerfSource1Table(Base):
    __tablename__ = "inner_source_1"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfSource2Table(Base):
    __tablename__ = "inner_source_2"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfTargetTable(Base):
    __tablename__ = "inner_target"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


def add_concat_transform(df1, df2):
    result = df1.copy()
    result['value'] = result['value'] + df2['value']
    result['category'] = result['category'] + ' ' + result['category']
    return result


pipeline = Pipeline([
    BatchTransform(
        add_concat_transform,
        inputs=[PerfSource1Table, PerfSource2Table],
        outputs=[PerfTargetTable],
        transform_keys=['id'],
        chunk_size=100
    )
])


def get_app() -> DatapipeApp:
    dbconn = get_dbconn()
    app = DatapipeApp(
        ds=DataStore(dbconn),
        catalog=Catalog({}),
        pipeline=pipeline
    )

    Base.metadata.create_all(dbconn.con)
    app.ds.meta_dbconn.sqla_metadata.create_all(dbconn.con)

    return app

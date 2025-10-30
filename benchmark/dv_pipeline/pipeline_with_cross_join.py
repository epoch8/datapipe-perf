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
    __tablename__ = "cross_source_1"

    id_1 = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfSource2Table(Base):
    __tablename__ = "cross_source_2"

    id_2 = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfTarget(Base):
    __tablename__ = "cross_target"

    id_1 = Column(String, primary_key=True)
    id_2 = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


def add_concat_transform(df1, df2):
    result = pd.merge(df1, df2, how='cross')
    result['value'] = result['value_x'] + result['value_y']
    result['category'] = result['category_x'] + ' ' + result['category_y']
    return result[['id_1', 'id_2', 'value', 'category']]


pipeline = Pipeline([
    BatchTransform(
        add_concat_transform,
        inputs=[PerfSource1Table, PerfSource2Table],
        outputs=[PerfTarget],
        transform_keys=['id_1', 'id_2'],
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

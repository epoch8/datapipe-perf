"""
Multi-table pipeline with join for testing PR #355 performance.
"""
import inspect
import pandas as pd
import sqlalchemy.orm as sa_orm
from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.types import JoinSpec
from sqlalchemy import Column, Integer, String

from benchmark.common import get_dbconn


class Base(sa_orm.DeclarativeBase):
    pass


class ProfilesTable(Base):
    __tablename__ = "perf_profiles"

    id = Column(String, primary_key=True)
    name = Column(String)
    age = Column(Integer)


class PostsTable(Base):
    __tablename__ = "perf_posts"

    id = Column(String, primary_key=True)
    user_id = Column(String)
    content = Column(String)


class JoinedResultTable(Base):
    __tablename__ = "perf_joined_result"

    id = Column(String, primary_key=True)
    user_id = Column(String)
    name = Column(String)
    age = Column(Integer)
    content = Column(String)


def join_transform(posts_df: pd.DataFrame, profiles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join posts with profiles using left join.

    IMPORTANT: Order changed! posts_df is now first (main table), profiles_df second.
    This allows filtered join optimization to work:
    - Posts table is the main table (where we track new records)
    - Profiles table is filtered based on user_id values from new posts
    """
    # Left join: posts with their profiles
    result = pd.merge(
        posts_df,
        profiles_df[["id", "name", "age"]],
        left_on="user_id",
        right_on="id",
        how="left",
        suffixes=("", "_profile")
    )

    # Keep post id as primary key
    result["id"] = result["id"].fillna(result["id_profile"])
    result = result.drop(columns=["id_profile"], errors="ignore")

    return result[["id", "user_id", "name", "age", "content"]]


def _supports_join_keys() -> bool:
    """Check if JoinSpec supports join_keys parameter."""
    try:
        sig = inspect.signature(JoinSpec.__init__)
        return 'join_keys' in sig.parameters
    except Exception:
        return False


def get_pipeline() -> Pipeline:
    """
    Create pipeline with or without join_keys optimization.

    Automatically detects if the current DataPipe version supports join_keys
    and creates the appropriate pipeline version.
    """
    supports_join_keys = _supports_join_keys()

    if supports_join_keys:
        # New version with filtered join optimization (Looky-7769/offsets-hybrid)
        return Pipeline([
            BatchTransform(
                join_transform,
                inputs=[
                    PostsTable,  # Main table (first argument to join_transform)
                    JoinSpec(
                        ProfilesTable,
                        join_keys={"user_id": "id"}  # Filter ProfilesTable.id by user_id from posts
                    )
                ],
                outputs=[JoinedResultTable],
                transform_keys=["id"],
                chunk_size=100,
            )
        ])
    else:
        # Old version without join_keys (master, PR-355-example)
        return Pipeline([
            BatchTransform(
                join_transform,
                inputs=[
                    PostsTable,  # Main table
                    ProfilesTable  # No JoinSpec filtering - reads all profiles
                ],
                outputs=[JoinedResultTable],
                transform_keys=["id"],
                chunk_size=100,
            )
        ])


# Create pipeline instance
pipeline = get_pipeline()


def get_pipeline_info() -> dict:
    """
    Get information about the current pipeline configuration.

    Returns:
        dict with:
        - supports_join_keys: bool - whether join_keys optimization is available
        - version: str - descriptive version name
        - optimization_enabled: bool - whether optimization is actually enabled
    """
    supports_join_keys = _supports_join_keys()
    return {
        "supports_join_keys": supports_join_keys,
        "version": "hybrid-with-join-keys" if supports_join_keys else "baseline-no-join-keys",
        "optimization_enabled": supports_join_keys,
        "description": (
            "Filtered join optimization (only loads matching profiles)"
            if supports_join_keys
            else "No join optimization (loads all profiles)"
        ),
    }


def get_app() -> DatapipeApp:
    """Get the DatapipeApp instance with multi-table pipeline."""
    dbconn = get_dbconn()
    app = DatapipeApp(
        ds=DataStore(dbconn),
        catalog=Catalog({}),
        pipeline=pipeline,
    )

    Base.metadata.create_all(dbconn.con)
    app.ds.meta_dbconn.sqla_metadata.create_all(dbconn.con)

    return app

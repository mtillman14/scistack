"""
GET /schema

Returns the experiment schema: the keys and their distinct values.
Used by the frontend to populate the global schema filter bar.
"""

from fastapi import APIRouter, Depends
from scidb.database import DatabaseManager
from scistack_gui.db import get_db

router = APIRouter()


@router.get("/schema")
def get_schema(db: DatabaseManager = Depends(get_db)):
    """
    Returns schema keys and all distinct values for each key.

    Example response:
        {
            "keys": ["subject", "session"],
            "values": {
                "subject": [1, 2, 3],
                "session": ["pre", "post"]
            }
        }
    """
    keys = db.dataset_schema_keys
    values = {key: db.distinct_schema_values(key) for key in keys}
    return {"keys": keys, "values": values}

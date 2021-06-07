import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
from bson.objectid import ObjectId
from constants.collections import MESSAGES

from constants.relationships import SUCCESS


@dag(
    "create-documents",
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["example"],
    catchup=False,
)
def create_documents_taskflow():
    @task
    def create() -> Dict[str, Any]:
        db: MongoHook = MongoHook("mongo")
        document = {
            "message": str(uuid.uuid4()),
            "created_at": datetime.now(),
        }
        insert_one_result = db.insert_one(MESSAGES, document)
        print(insert_one_result)
        return {SUCCESS: [str(insert_one_result.inserted_id)]}

    @task
    def read(inserted_ids) -> Dict[str, Any]:
        db: MongoHook = MongoHook("mongo")
        documents = db.find(
            MESSAGES, {"_id": {"$in": [ObjectId(id) for id in inserted_ids]}}
        )
        ids = [str(document["_id"]) for document in documents]
        return {SUCCESS: ids}

    @task
    def update(ids) -> Dict[str, Any]:
        db = MongoHook("mongo")
        db.update_many(
            MESSAGES,
            {"_id": {"$in": [ObjectId(_id) for _id in ids]}},
            {"$set": {"updated_at": datetime.now()}},
        )
        return {SUCCESS: ids}

    @task
    def delete(ids) -> Dict[str, Any]:
        db = MongoHook("mongo")
        db.delete_many(
            MESSAGES,
            {"_id": {"$in": [ObjectId(_id) for _id in ids]}},
        )
        return {SUCCESS: ids}

    results_create = create()
    results_read = read(results_create[SUCCESS])
    results_update = update(results_read[SUCCESS])
    delete(results_update[SUCCESS])


create_documents_dag = create_documents_taskflow()

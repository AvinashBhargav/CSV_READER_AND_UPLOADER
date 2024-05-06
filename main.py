from fastapi import FastAPI, HTTPException, UploadFile, File, status
from pydantic import BaseModel
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import List, Optional

import csv

from pymongo.server_api import ServerApi
from starlette.responses import JSONResponse

# MongoDBCompass
#client = MongoClient("mongodb://localhost:####")
#db = client["abdb"]
#collection = db["a1"]


#MongoAtlas
uri = "mongodb+srv://admin:<password>@cluster0.w2dqhlv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri)
db = client['db1']
collection = db['ab']

app = FastAPI()


class Item(BaseModel):
    id: int
    UserID : str
    First_Name : str
    Last_Name: str
    Sex: str
    Email: str
    Phone: str
    DOB: str
    Job_Title : str


@app.get("/items/", response_model=List[Item])
async def get_all_items():
    items = collection.find()
    return list(items)


# CRUD operations
@app.post("/items/", response_model=Item)
async def create_item(item: Item):
    existing_item = collection.find_one({"id": item.id})
    if existing_item:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Item with this id already exists")

    result = collection.insert_one(item.dict())
    created_item = collection.find_one({"_id": result.inserted_id})
    return created_item


@app.get("/items/{item_id}", response_model=Item)
async def read_item(item_id: int):
    item = collection.find_one({"id": item_id})
    if item:
        return item
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")


@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: int, item: Item):
    existing_item = collection.find_one({"id": item_id})
    if not existing_item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    result = collection.update_one({"id": item_id}, {"$set": item.dict()})
    if result.modified_count == 1:
        updated_item = collection.find_one({"id": item_id})
        return updated_item
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update item")


@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    deletion_result = collection.delete_one({"id": item_id})
    if deletion_result.deleted_count == 1:
        return {"message": "Item deleted successfully"}
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")


@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        contents = await file.read()
        decoded_contents = contents.decode('utf-8').splitlines()
        items = []
        try:

            # Assuming CSV format: id,name,author,description
            reader = csv.DictReader(decoded_contents)
            for row in reader:
                # noinspection PyTypeChecker
                item = Item(
                    id=int(row['id']),  # Default value
                    UserID= row['UserID'],
                    First_Name=row['First_Name'],
                    Last_Name=row['Last_Name'],
                    Sex=row['Sex'],
                    Email=row["Email"],
                    Phone=row['Phone'],
                    DOB=row['DOB'],
                    Job_Title=row['Job_Title']
                )
                items.append(item.dict())
        except csv.Error as e:
            raise HTTPException(status_code=status.HTTP_404_BAD_REQUEST, detail=f"CSV Error: {str(e)}")
        try:
            collection.insert_many(items)
        except Exception:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error in saving database")

        return JSONResponse(content={"message": "File uploaded Successufully"})


@app.post("/Multipleuploadfiles/")
async def upload_files(files: List[UploadFile] = File(...)):
    items = []
    for file in files:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only CSV files are allowed")

        contents = await file.read()
        decoded_contents = contents.decode('utf-8').splitlines()
        file_items = []
        try:
            reader = csv.DictReader(decoded_contents)
            for row in reader:
                item = Item(
                    id=int(row['id']),  # Default value
                    UserID=row['UserID'],
                    First_Name=row['First Name'],
                    Last_Name=row['Last Name'],
                    Sex=row['Sex'],
                    Email=row["Email"],
                    Phone=row['Phone'],
                    DOB=row['DOB'],
                    Job_Title=row['Job_Title']
                )
                file_items.append(item.dict())
        except UnicodeDecodeError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error decoding CSV: {str(e)}")
        except csv.Error as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"CSV Error: {str(e)}")

        items.extend(file_items)

    try:
        collection.insert_many(items)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error saving to database: {str(e)}")

    return JSONResponse(content={"message": "Files uploaded successfully"})

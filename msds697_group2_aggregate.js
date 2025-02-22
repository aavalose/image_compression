db.raw_data.aggregate([
  {
    "$project": {
      "id": 1,
      "url": 1,
      "short_url": 1,
      "views": 1,
      "favorites": 1,
      "source": 1,
      "purity": 1,
      "category": 1,
      "dimension_x": { "$ifNull": ["$dimension_x", "$width"] },
      "dimension_y": { "$ifNull": ["$dimension_y", "$height"] },
      "resolution": 1,
      "ratio": 1,
      "file_size": 1,
      "file_type": 1,
      "created_at": 1,
      "colors": 1,
      "path": 1,
      "thumbs": 1,
      "area": {
        "$multiply": [
          { "$ifNull": ["$dimension_x", "$width"] },
          { "$ifNull": ["$dimension_y", "$height"] }
        ]
      }
    }
  },
  {
    "$addFields": {
      "photo_size": {
        "$cond": {
          "if": { "$gt": ["$area", 480000] },
          "then": "medium",
          "else": "small"
        }
      }
    }
  },
  {
    "$project": {
      "area": 0
    }
  },
  {
    "$out": "image_sized"
  }
])

//queries

db["image_sized"].find().count() //output: 15
db["image_sized"].find().sort({'dimension_x' : -1}) //sorts the documents by 'dimension_x' in descending order

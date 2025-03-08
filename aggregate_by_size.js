db.raw_data.aggregate([
  // 1) Project to rename `_id` -> `raw_data_id` and `id` -> `API_id`.
  {
    "$project": {
      "API_id": "$id",
      "dimension_x": { "$ifNull": ["$dimension_x", "$width"] },
      "dimension_y": { "$ifNull": ["$dimension_y", "$height"] },
      "url": { "$ifNull": ["$urls.regular", "$src.original"] },
      "title": { "$ifNull": ["$slug", "$alt"] }
    }
  },
  // 2) Add `area` field for dimension_x * dimension_y.
  {
    "$addFields": {
      "area": { "$multiply": ["$dimension_x", "$dimension_y"] }
    }
  },
  // 3) Derive `photo_size` based on `area`.
  {
    "$addFields": {
      "photo_size": {
        "$cond": {
          "if": { "$gt": ["$area", 10000000] },
          "then": "medium",
          "else": "small"
        }
      }
    }
  },
  // 4) Add `origin_API` field to detect if url is from pexels or unsplash.
  {
    "$addFields": {
      "origin_API": {
        "$switch": {
          "branches": [
            {
              "case": { "$regexMatch": { "input": "$url", "regex": "pexels" } },
              "then": "pexels"
            },
            {
              "case": { "$regexMatch": { "input": "$url", "regex": "unsplash" } },
              "then": "unsplash"
            }
          ],
          "default": "other"
        }
      }
    }
  },
  // 5) Final projection: keep only the fields we need in the output.
  {
    "$project": {
      "raw_data_id": 1,
      "API_id": 1,
      "dimension_x": 1,
      "dimension_y": 1,
      "area": 1,
      "url": 1,
      "title": 1,
      "photo_size": 1,
      "origin_API": 1
    }
  },
  // 6) Output to `image_sized` collection.
  {
    "$out": "image_sized"
  }
])
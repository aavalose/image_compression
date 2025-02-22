db["image_sized"].find().count() //output: 15
db["image_sized"].find().sort({'dimension_x' : -1}) //sorts the documents by 'dimension_x' in descending order

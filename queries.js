   //TASK 2

//Find Fantasy genre
db.books.find({ genre: "Fantasy"})

//Books published before 1940
 db.books.find({ published_year: { $lt: 1920 } })

 //Books written by 
 db.books.find({ author: "Herman Melville" })

 //Update the price of Moby Dick
 db.books.updateOne({ title: "Moby Dick" }, { $set: { price: 120.3 }})

 //Delete the Wrthering Heights
 db.books.deleteOne({ title: "Wrthering Heights" })

  

     //TASK 3

//Write a query to find books that are both in stock and published after 2010
db.books.find({ in_stock: true, published_year: { $gt: 2010 } })

//projection to return only the title, author, and price 
db.books.find({}, {
    title: 1,
    author: 1,
    price: 1,
     })

//sorting to display books by price ascending
db.books.find().sort({ price: 1 });

//sorting to display books by price descending
db.books.find().sort({ price: -1 });

//Using the `limit` and `skip` methods to implement pagination (5 books per page)
 //first 5 pages 
db.books.find().limit(5).skip(0);

 //second 5 pages 
db.books.find().limit(5).skip(5);

 //third 5 pages 
db.books.find().limit(5).skip(10);
 

       //TASK 4

//an aggregation pipeline to calculate the average price of books by genre
db.books.aggregate([ { $group: { _id: "$genre", avgPrice: { $avg: "$price" } }} ])


//an aggregation pipeline to find the author with the most books in the collection
db.books.aggregate([ { $group: { _id: "$author", totalBooBooks: { $sum: 1 } }}, { $sort: { totalBooks: -1 } }, { $limit: 1 } ])

//Aggregation pipelines that transform and analyze the data
  // 1 Author productivity analysis based on pages and books 

  db.books.aggregate([
  {
    $group: {
      _id: "$author",
      totalBooks: { $sum: 1 },
      avgPages: { $avg: "$pages" }
    }
  },
  {
    $match: { totalBooks: { $gt: 1 } } // Only authors with multiple books
  },
  {
    $sort: { totalBooks: -1 }
  }
])

//2 Publication trends 
db.books.aggregate([
  {
    $group: {
      _id: { $year: "$published_date" },
      booksPublished: { $sum: 1 },
      genres: { $addToSet: "$genre" }
    }
  },
  {
    $sort: { "_id": 1 } // Sort by year
  }
])

//3 publisher analysis 
db.books.aggregate([
  {
    $group: {
      _id: "$publisher",
      totalRevenue: { $sum: "$price" },
      avgRating: { $avg: "$rating" },
      flagshipTitle: { $first: "$title" }
    }
  },
  {
    $project: {
      _id: 0,
      publisher: "$_id",
      totalRevenue: 1,
      avgRating: { $round: ["$avgRating", 2] },
      flagshipTitle: 1
    }
  },
  {
    $sort: { totalRevenue: -1 }
  }
])


//Properly implemented indexes with performance analysis
   //1 Author index 
db.books.createIndex({ author: 1 }, { name: "author_idx" });

   //2 Publication year index 
db.books.createIndex({ published_year: -1 }, { name: "pub_year_desc_idx" });

   //3 text search index 
db.books.createIndex(
  { title: "text", author: "text" },
  { name: "book_search_idx", weights: { title: 3, author: 1 } }
);


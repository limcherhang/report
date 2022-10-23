db.order.find({
    "createtime" : {$gte:ISODate("2022-10-18T00:00:00.000+00:00"), $lt:ISODate("2022-10-19T00:00:00.000+00:00")},
    // "gamecode":"1",
    // "account":"hqq1234560wnsrad"
})
   .projection({})
   .sort({gametoken:1, bettime:-1})
   .limit(100)
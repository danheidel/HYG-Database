var mariadb = require('mysql');
var dbCon = require('./foo');

dbConnection = dbCon.dbConHandler({
  host: 'localhost',
  user: 'root',
  password: 'Ih35MV9XqLcS',
  database: 'HYG',
});
  
dbConnection.query('SELECT * FROM good_dist WHERE Distance < 2', function(err, rows){
  if(err){
    console.log(err);
  }else{
    console.log(rows);
    console.log(rows.length + ' rows returned');
    dbCon.closeDbConnection(dbConnection);
  }
});

function closeDbConnection(){
  mariaConnection.end(function(err){
    if(err){
      console.log(err);
    }
    console.log('database connection closed');
  });
}

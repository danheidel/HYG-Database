var fs = require('fs');
var csv = require('csv');
var mariadb = require('mysql');
var fileName = '/hygfull.csv';

function mariaConHandler() {
        mariaConnection = mariadb.createConnection({
                host:   "localhost",
                user:   "root",
                password:"Ih35MV9XqLcS",
                database:"HYG",
        });

        mariaConnection.connect(function(err){
                if(err){
                        console.log('error while connecting to db: ', err);
                        setTimeout(mariaConHandler, 2500);
                }else{
			console.log('database connection opened');
		}
        });

        mariaConnection.on('error', function(err){
                console.log('db error: ', err);
                if(err.code ==='PROTOCOL_CONNECTION_LOST'){
                        mariaConHandler();
                } else {
                        throw err;
                }
        });
}

mariaConHandler();

var csvArray = [];

var spectrumConv = [
  {'b-v':-0.30,sp:''},
  {'b-v':-0.28,sp:''},
  {'b-v':-0.26,sp:''},
  {'b-v':-0.25,sp:''},
  {'b-v':-0.24,sp:''},
  {'b-v':-0.22,sp:''},
  {'b-v':-0.20,sp:''},
  {'b-v':-0.19,sp:''},
  {'b-v':-0.18,sp:''},
  {'b-v':-0.18,sp:''},
  {'b-v':-0.17,sp:''},
  {'b-v':-0.16,sp:''},
  {'b-v':-0.14,sp:''},
  {'b-v':-0.13,sp:''},
  {'b-v':-0.12,sp:''},
  {'b-v':-0.11,sp:''},
  {'b-v':-0.09,sp:''},
  {'b-v':-0.07,sp:''},
  {'b-v':-0.04,sp:''},
  {'b-v':-0.01,sp:''},
  {'b-v':0.02,sp:''},
  {'b-v':0.05,sp:''},
  {'b-v':0.08,sp:''},
  {'b-v':0.12,sp:''},
  {'b-v':0.15,sp:''},
  {'b-v':0.17,sp:''},
  {'b-v':0.20,sp:''},
  {'b-v':0.27,sp:''},
  {'b-v':0.30,sp:''},
  {'b-v':0.32,sp:''},
  {'b-v':0.34,sp:''},
  {'b-v':0.35,sp:''},
  {'b-v':0.45,sp:''},
  {'b-v':0.53,sp:''},
  {'b-v':0.60,sp:''},
  {'b-v':0.63,sp:''},
  {'b-v':0.65,sp:''},
  {'b-v':0.68,sp:''},
  {'b-v':0.74,sp:''},
  {'b-v':0.81,sp:''},
  {'b-v':0.86,sp:''},
  {'b-v':0.92,sp:''},
  {'b-v':0.95,sp:''},
  {'b-v':,sp:  {'b-v':,sp:''},
  {'b-v':,sp:''},
  {'b-v':,sp:''},
  {'b-v':,sp:''},
  {'b-v':,sp:''},
''},
  {'b-v':,sp:''},
  {'b-v':,sp:''},
];

csv().from.path(__dirname + fileName, {delimiter:','})
.on('record', function(row, index){
  var tempData = {
    StarID: row[0],
    Hip: row[1]==''?null:row[1],
    HD: row[2]==''?null:row[2],
    HR: row[3]==''?null:row[3],
    Gliese: row[4]==''?null:row[4],
    BayerFlamstead: row[5]==''?null:row[5],
    ProperName: row[6]==''?null:row[6],
    RA: row[7]==''?null:row[7],
    Dec_: row[8]==''?null:row[8],
    Distance: row[9]==''?null:row[9],
    Mag: row[10]==''?null:row[10],
    AbsMag: row[11]==''?null:row[11],
    Spectrum: row[12]==''?null:row[12],
    ColorIndex: row[13]==''?null:row[13],
  };
  
  csvArray.push(tempData);
  if(index % 1000 == 0){console.log(index);}
})
.on('end', function(count){
  console.log(count + ' rows added');
  console.log('array has: ' + csvArray.length + ' entries');
  writeToDb(1); //fudge up by one to lose the column headers
  //closeDbConnection();
})
.on('error', function(error){
  console.log(error.message);
});

function writeToDb(rep){
  if(rep < csvArray.length){
    dbInsert(rep, function(){ writeToDb(rep + 1);});
  }else{
    console.log('wrote ' + rep + ' rows to db');
    closeDbConnection();
  }
}

function dbInsert(rep, callback){
  mariaConnection.query('INSERT INTO full SET ?', csvArray[rep], function(err, result){
    if(err){console.log(err);}
    if(rep%100 == 0){console.log(rep);}
    callback();
  });  
}

function closeDbConnection(){
  mariaConnection.end(function(err){
    if(err){
      console.log(err);
    }
    console.log('database connection closed');
  });
}

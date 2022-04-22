const decimalBase = 1000000000000000000;
var mongo = require('mongodb');
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";
var axios = require('axios')
var axiosRetry =require('axios-retry');
axiosRetry(axios, { retries: 3 });

const schedule = require('node-schedule');
const rule = new schedule.RecurrenceRule();
rule.hour = 20;
rule.minute = 00;

const myConfig = {
    raxConfig: {
      retry: 5, // number of retry when facing 4xx or 5xx
      noResponseRetries: 5, // number of retry when facing connection error
      onRetryAttempt: err => {
        const cfg = rax.getConfig(err);
        console.log(`Retry attempt #${cfg.currentRetryAttempt}`); // track current trial
      }
    },
    timeout: 50 // don't forget this one
  }
const minerList=[
    'f092228',
'f0130884',
'f0112667',
'f0155050',
'f0402661',
'f0672951',
'f01602479',
'f01606675',
'f01606849',
'f01641612',
'f01238519',
'f01264125',
'f01466173',
'f01694304',
'f01731371',
'f01776738',
'f01372912',
'f01479781',
'f01662887',
'f01716466',
'f01716454'
]
//insert document to a collection
function insertManyDocument(url,document, myObj){
    MongoClient.connect(url, function(err, db) {
      if (err) throw err;
      // var myobj = { name: "Company Inc", address: "Highway 37" };
      var dbo = db.db("filfox");
      dbo.collection(document).insertMany(myObj, function(err, res) {
        if (err) throw err;
        console.log("many document inserted");
        db.close();
      });
    });
  }
 //insert one document to a collection
function insertOneDocument(url,document, myObj){
    MongoClient.connect(url, function(err, db) {
      if (err) throw err;
      // var myobj = { name: "Company Inc", address: "Highway 37" };
      var dbo = db.db("filfox");
      dbo.collection(document).insertOne(myObj, function(err, res) {
        if (err) throw err;
        console.log("1 document inserted");
        db.close();
      });
    });
  } 

  function find(url,document, query,callback){
    MongoClient.connect(url, function(err, db) {
      if (err) throw err;
      // var myobj = { name: "Company Inc", address: "Highway 37" };
      var dbo = db.db("filfox");
      dbo.collection(document).findOne(query, function(err, res) {
        if (err) throw err;
        // console.log("document found",res);
        db.close();
        callback(query.miner,res["worker"],1646110800,0)
      });
    });
  } 
  
async function getMessagesFromFilfox(address){
    // 计算需要发送多少次请求取得全部信息
   var res=  await axios.get(`https://filfox.info/api/v1/address/${address}/messages?pageSize=100&page=1`)
   let data = await res.data;
   var messageToSave= { }
   var messagesTotal =data['totalCount']
   var pageTotal = parseInt(messagesTotal/100)
   console.log('page total is',pageTotal)
  // 取得messages
   let messages = data["messages"]
   var messageToSave=[]
    var message
    var gasfee
    var messageCid
   for( var i=1;i<=100;i++){ 
    //    console.log(messages[i])
        console.log('loop position is ',i)
        messageCid= messages[i]["cid"]
        gasfee = await getGasFromMessage(messageCid)
        newRecord = {
            "miner": address,
            "cid" : messageCid,
            "timestamp":messages[i]["timestamp"],
            "from" :messages[i]["from"],
            "to":messages[i]["to"],
            "nonce":messages[i]["nonce"],
            "value" : messages[i]["value"],
            "method" : messages[i]["method"],
            "minerFee" :gasfee["minerFee"],
            "burnFee" : gasfee["burnFee"],
            "gasUsed" :gasfee["gasUsed"]
        }
        
        messageToSave.push(newRecord)
    }
    insertDocument(url,messageToSave)
//   db.close()
// 日期格式转换 epoch -> string
//     var dstamp = new Date(0); // The 0 there is the key, which sets the date to the epoch
//    dstamp.setUTCSeconds(timestamp);
//    let day = dstamp.toLocaleDateString()
//    console.log(dstamp)
   
}

// 日期格式转化为可读
function convertDateFormat(dateObj) {
    var s,d;
    d = new Date(0);
    d.setUTCSeconds(dateObj);
    // s = d.getUTCFullYear()+ "/";
    // s += d.getUTCMonth() + 1+ "/"; //months from 1-12
    // s += d.getUTCDate();
    return d;
}
// get miner data from filfox api, and save to database 'filfox'-'minerData'
async function getMinerData(address){
    console.log('address ',address)
    var res = await axios.get(`https://filfox.info/api/v1/address/${address}`)
    var data = await res.data
    // var actor = data["actor"]
    // console.log(address,actor)
    // var minerData={}
    // 
    console.log(data)
    var date = data["timestamp"]
    data.date=convertDateFormat(date)
    //convert date to a readable formate
    insertOneDocument(url,'minerData',data)
}
async function getGasFromMessage(messageCId){
    console.log('function getGasFromMessage ')
    console.log('cid',messageCId)
    var res = await axios.get(`https://filfox.info/api/v1/message/${messageCId}`)
    var data = await res.data
    var transactions = data["transfers"]
     var minerFee = transactions[0]["value"]
     var burnFee = transactions[1]["value"]
     var gasUsed = data["receipt"]["gasUsed"]
    return {
        "minerFee":minerFee,
        "burnFee":burnFee,
        "gasUsed":gasUsed
    }
    
}

//function  add miner into the transaction
function addMiner(transactionList,miner){
    transactionList.forEach((transaction)=>{
        transaction.miner= miner
    })
}

// 日期格式转化为可读
function convertDateFormat(dateObj) {
    var s,d;
    d = new Date(0);
    d.setUTCSeconds(dateObj);
    // s = d.getUTCFullYear()+ "/";
    // s += d.getUTCMonth() + 1+ "/"; //months from 1-12
    // s += d.getUTCDate();
    return d;
}
//process transactions
function processTransaction(transaction){
    // console.log('processTransaction')
    transaction["value"]=parseInt(transaction["value"])/decimalBase
    var dateObj = new Date(0); // The 0 there is the key, which sets the date to the epoch
    dateObj.setUTCSeconds(transaction["timestamp"]);
    transaction.date= dateObj
    transaction.month = dateObj.getUTCMonth() + 1; //months from 1-12
    transaction.day = dateObj.getUTCDate();
    transaction.year = dateObj.getUTCFullYear();
    // console.log(transaction)
    // console.log('end of addDMYtoTransaction')
}

//get transaction list from filfox
async function getTransactionsOfMiner(address,type){
    console.log('type ',type)
    var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=0&type=${type}`)
    var data = await res.data
    //save the first page data into mongodb
     //calculate the size of pages in total,
     var pageSize = parseInt(data["totalCount"]/100)+1
     var timestamp
    if( data["totalCount"] >0){
        for(var i=0;i<data["transfers"].length;i++){
            data["transfers"][i].miner=address
            /
            processTransaction(data["transfers"][i])
            timestamp= convertDateFormat( data["transfers"][i]["timestamp"])
            data["transfers"][i].timestamp=timestamp
            console.log("i=",i)
         }
        insertManyDocument(url,'transactions',data["transfers"])
    }
    
    if(pageSize>1){
        console.log("pagesize >1")

        for(var i=1;i<=pageSize;i++){
            console.log('read next page')
            var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${i}&type=${type}`)
            data = await res.data
           
            //save the first page data into mongodb
            console.log('i=',i)
            for(var j=0;j<data["transfers"].length;j++){
                // console.log("j=",j)
                data["transfers"][j].miner=address
                processTransaction(data["transfers"][j])
                timestamp= convertDateFormat( data["transfers"][i]["timestamp"])
                data["transfers"][i].timestamp=timestamp
             }
            insertManyDocument(url,'transactions',data["transfers"])
        }
    }
    
}
//get all the miners' transactions
function getAllMinersTransactions(){
    const minerList=[
        'f092228',
        'f0130884',
        'f0112667',
        'f0155050',
        'f0402661',
        'f0672951',
        'f01602479',
        'f01606675',
        'f01606849',
        'f01641612',
        'f01238519',
        'f01264125',
        'f01466173',
        'f01694304',
        'f01731371',
        'f01776738',
        'f01372912',
        'f01479781',
        'f01662887',
        'f01716466',
        'f01716454',
    ]
    for(var i=0; i<=minerList.length;i++){
        getTransactions(minerList[i])
        console.log('miner is ',minerList[i])
    }
    // minerList.forEach((miner)=>{
    //     console.log('miner is :',miner)
    //     getTransactions(miner)
    // })
}

// getMessagesFromFilfox('f0130884')
// get all miners data 
async function getWorkerContOfMiner(address){
    console.log('address ',address)
    var res = await axios.get(`https://filfox.info/api/v1/address/${address}`)
    var data = await res.data
    var workerContro={}
    
    workerContro["miner"] = address
   
    workerContro.worker = data["miner"]["worker"]["address"]
    var controllers = data["miner"]["controlAddresses"]
    
    var contrArray =[]
    controllers.forEach((con)=>{
        contrArray.push(con.address)
    })
    workerContro.controlAddresses = contrArray
    console.log(workerContro)
    insertOneDocument(url,'workerController',workerContro)

}
function getWorkerContOfMinerAll(){
    
    minerList.forEach((miner)=>{
        getWorkerContOfMiner(miner)
    })
}
function getMinerDataList(){
    console.log('getMinerDataList!')
    var minerDataList = []
    
    minerList.forEach((miner)=>{
        getMinerData(miner)
    })
   
}
async function getTransactions(address){
    console.log('address',address)
    // console.log('type ',type)
    var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=0`)
    var data = await res.data
    //save the first page data into mongodb
     //calculate the size of pages in total,
     var pageSize = parseInt(data["totalCount"]/100)
     var timestamp
    if( data["totalCount"] >0){
        //page start from 0
        for(var i=4964;i>=0;i--){
            console.log("page",i)
            //i is page number
            res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${i}`)
            data = await res.data
            // console.log(data)
            for(var j=0;j<data["transfers"].length;j++){
                //j is transaction number
                // console.log("transaction",j)
                data["transfers"][j].miner=address
                data["transfers"][j].address=address
                processTransaction(data["transfers"][j])
             }
             //insert the transaction of this page into db
             insertManyDocument(url,'transactions',data["transfers"])
        }
    }
}

const getTransWithTimestampWorker =async (miner,address,maxTimestamp,startPage)=>{
    // console.log('type ',type)
    try{
        var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=0`)
    }catch(error){
        console.log(error)
    }
    var data = await res.data
    //save the first page data into mongodb
     //calculate the size of pages in total,
     var pageSize = parseInt(data["totalCount"]/100)
     var timestamp
     var continueLoop = true
    if( data["totalCount"] >0){
        //page start from 0
        for(var i=startPage;i<=pageSize;i++){
           if(continueLoop){
                   console.log("page",i)
               //i is page number
               try{
                   res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${i}`)
                }catch(error){
                    getTransWithTimestampWorker(miner,address,maxTimestamp,i)
                console.log('error',error)
            }
               data = await res.data
               // console.log(data)
            if (data["transfers"].length > 0){
                for(var j=0;j<data["transfers"].length;j++){
                    //j is transaction number
                    timestamp = data["transfers"][j].timestamp
                    console.log('timestamp',timestamp)
                    if(timestamp > maxTimestamp){
                        console.log("transaction",j)
                        data["transfers"][j].miner=miner
                        data["transfers"][j].address=address
                        processTransaction(data["transfers"][j])
                    }else{
                        continueLoop =false
                        data["transfers"].splice(j,1)
                    }
                 }
              //insert the transaction of this page into db
              if(data["transfers"].length>0){
                  insertManyDocument(url,'transactions',data["transfers"])
              }
            }
           }else{
               console.log('timestapm less than maxstamp')
               //update maxtimestamp
            //    updateMaxMin(address)
               break;
           }
       }
    }
}
async function getTransactionsWorker(address){
    console.log('miner',address)
    // console.log('type ',type)
    //  取得此miner的worker 地址和controller地址
    find(url,'workerController',{miner:address},getTransWithTimestampWorker)
}

async function getTransactionsPage(address,pageNumber){
    console.log('address',address)
    console.log('page number',pageNumber)
    // console.log('type ',type)
    res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${pageNumber}`)
    data = await res.data
    // console.log(data)
    for(var j=0;j<data["transfers"].length;j++){
        console.log(data[j])
        //j is transaction number
        data["transfers"][j].miner=address
        data["transfers"][j].address=address
        processTransaction(data["transfers"][j])
     }
     //insert the transaction of this page into db
     insertManyDocument(url,'transactions',data["transfers"])
}
async function getTransWithTimestamp(address,maxTimestamp){
     // console.log('type ',type)
     var res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=0`)
     var data = await res.data
     //save the first page data into mongodb
      //calculate the size of pages in total,
      var pageSize = parseInt(data["totalCount"]/100)
      var timestamp
      var continueLoop = true
     if( data["totalCount"] >0){
         //page start from 0
         for(var i=0;i<=pageSize;i++){
            if(continueLoop){
                    console.log("page",i)
                //i is page number
                res = await axios.get(`https://filfox.info/api/v1/address/${address}/transfers?pageSize=100&page=${i}`)
                data = await res.data
                // console.log(data)
                for(var j=0;j<data["transfers"].length;j++){
                    //j is transaction number
                    timestamp = data["transfers"][j].timestamp
                    console.log('timestamp',timestamp)
                    if(timestamp > maxTimestamp){
                        console.log("transaction",j)
                        data["transfers"][j].miner=address
                        data["transfers"][j].address=address
                        processTransaction(data["transfers"][j])
                    }else{
                        continueLoop =false
                        data["transfers"].splice(j,1)

                    }
                 }
              //insert the transaction of this page into db
              insertManyDocument(url,'transactions',data["transfers"])
            }else{
                break;
            }
        }
             
     }
}
// const job = schedule.scheduleJob(rule, function(){
    // getAllMinersTransactions()
  
    // getTransactions('f0130884') done
    // getTransactions('f092228')  done

    //page 1242,3855,  {miner:"f0112667",timestamp:1650290490}
    // getTransactionsPage('f0112667',3854) 
    // getTransactions('f0112667')  done
    // getTransactions('f0155050',)   下载重复多几十条
    // getTransactions('f0402661') done
    // getTransactions('f0672951')  done
    // getTransactions('f01602479') done
    // getTransactions('f01606675') done
    // getTransactions('f01606849') done
    // getTransactions('f01641612') done
    // getTransactions('f01238519') done
   
    // getTransactions('f01264125') 
    // getTransactions('f01466173') done
    // getTransactions('f01694304') done
    // getTransactions('f01731371') done
    // getTransactions('f01776738') done
    // getTransactions('f01372912') done
    // getTransactions('f01479781') done
    // getTransactions('f01662887') done
    // getTransactions('f01716466') done
    // getTransactions('f01716454') 无
    // getTransWithTimestamp('f0130884',1649097420)

// find(url,'workerController',{miner:'f0155050'})
// getTransactionsWorker('f01716466')  next 1444
getTransactionsWorker('f092228') 
    // getWorkerContOfMinerAll()
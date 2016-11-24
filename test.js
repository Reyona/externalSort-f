var async = require('async');
var fs = require('fs');
var writeLine = require('lei-stream').writeLine;
var lineReader = require('line-reader');
var inputFile = './input.txt';
var outputFile = './output.txt';
var length = 500;

function initData(callback){
    /*if unordered data exists, skip this step*/
    fs.exists(inputFile, function(exists){
        if(exists) {
            console.log('init skip');
            callback();
        }
        else {
            var initInput = writeLine(inputFile);
            var data;
            for (var i = 0; i < length; i++) {
                data={
                    id:Math.floor(Math.random() * length),
                    val:'test'
                };
                initInput.write(JSON.stringify(data));
                initInput.flush();
            }
            initInput.end(function () {
                console.log('init done');
                callback();
            });
        }
    });
}

function sort(callback){
    var begin = new Date().getTime();
    externalSort.externalSort(
        inputFile,
        function(a,b){
            return a['id'] - b['id'];//compare function+++++++
        },
        function(output){
            outputFile=output;
            console.log('Time cost: '+ (new Date().getTime() - begin)+' ms');
            callback();
        }
    );
}

function validation(callback) {
    console.log('validating...');
    var lastData=0;
    var currentData=0;
    lineReader.open(outputFile, function (err, reader) {
        if (err) throw err;
        function loopSplit() {
            if (reader.hasNextLine()) {
                reader.nextLine(function (err, data) {
                    currentData = (JSON.parse(data))['id'];
                    if(currentData>=lastData){
                        lastData=currentData;
                        loopSplit();
                    }
                    else {
                        callback(null,false);
                    }
                });
            }
            else {
                callback(null,true);
            }
        }
        loopSplit();
    });
}

var externalSort = require('external-sort');
async.series([initData,sort,validation], function (err, result) {
    console.log('validation: '+result[2]);
});

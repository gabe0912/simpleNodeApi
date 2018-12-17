const express = require('express');
const Druid = require('druid-query');
const Client = Druid.Client;
const Query = Druid.Query;
const client = new Client('http://127.0.0.1:8080');
const port = 3000;
const app = module.exports = express();
const mockData = require('./mockTimeseriesDBData.js');


const getMessagesBySrcInteval = (req) => {
    var src_id = req.query.src_id;
    var start_date = req.query.start_date;
    var end_date = req.query.end_date;
    var interval = req.query.interval;

    rawDruidQuery = client.timeseries()
        .dataSource('mockDatasource')
        .granularity('duration',interval * 60 * 1000, origin=start_date) //{interval} min * 60 seconds *1000 millis
        .filter('and',[Query.filter('selector','src_id',src_id)])
        .aggregation('count','message_count','src_id')
        .aggregation('doubleSum','sum_latitude','latitude')
        .aggregation('doubleSum','sum_longitude','longitude')
        .aggregation('doubleSum','sum_ir_thermo_temperature_filtered','ir_thermo_temperature_filtered')
        .aggregation('doubleSum','sum_relative_humidity','relative_humidity')
        .aggregation('doubleSum','sum_air_temp','air_temp')
        .aggregation('doubleSum','sum_wind_speed_world_filtered','wind_speed_world_filtered')
        .postAggregation('arithmetic','average_latitude','/',
            [
                Query.postAggregation('fieldAccess','avg_latitude','sum_latitude'),
                Query.postAggregation('fieldAccess','avg_longitude','sum_longitude')
            ])
        .postAggregation('arithmetic','average_ir_thermo_temperature_filtered','/',
            [
                Query.postAggregation('fieldAccess','avg_ir_thermo_temperature_filtered','sum_ir_thermo_temperature_filtered'),
                Query.postAggregation('fieldAccess','count_messages','message_count')
            ])
        .postAggregation('arithmetic','average_relative_humidity','/',
            [
                Query.postAggregation('fieldAccess','avg_relative_humidity','sum_relative_humidity'),
                Query.postAggregation('fieldAccess','count_messages','message_count')
            ])
        .postAggregation('arithmetic','average_air_temp','/',
            [
                Query.postAggregation('fieldAccess','avg_air_temp','sum_air_temp'),
                Query.postAggregation('fieldAccess','count_messages','message_count')
            ])
        .postAggregation('arithmetic','average_wind_speed_world_filtered','/',
            [
                Query.postAggregation('fieldAccess','avg_wind_speed_world_filtered','sum_wind_speed_world_filtered'),
                Query.postAggregation('fieldAccess','count_messages','message_count')
            ])
        .interval(Date.parse(start_date),Date.parse(end_date))

    rawDruidQuery.exec(function (err, result) {
        if (err){
            //Since there is no DB instantiated, this path will always be invoked so we continue to mock data below
        } else {
            return result
        }
    });
    console.log('rawDruidQuery: '+ JSON.stringify(rawDruidQuery));
    return mockData.getMockTimeseries(src_id, start_date, end_date, interval);
}

const validateQueryParams = (req, res, next) => {
    console.log('validating query params ');

    if(!req.query.src_id) {
        console.log(req.query.src_id)
        return res.status(500).json({
            code: '500',
            message: 'Request missing src_id'
        });
    }
    if(!req.query.start_date) {
        return res.status(500).json({
            code: '500',
            message: 'Request missing start_date'
        });
    }
    if(!req.query.end_date) {
        return res.status(500).json({
            code: '500',
            message: 'Request missing end_date'
        });
    }
    if(!req.query.interval) {
        return res.status(500).json({
            code: '500',
            message: 'Request missing interval'
        });
    }
    return next();
};

app.use(express.urlencoded({
    extended: true
}));

app.get('/messages', validateQueryParams, (req, res, next) => {
    const elements = getMessagesBySrcInteval(req);
    if(!elements) {
        return next();
    }
    return res.status(200).json({
        code: '200',
        body: elements
    });
});

if(!module.parent) {
    app.listen(port);
    console.log('Listening on port ' + port);
}
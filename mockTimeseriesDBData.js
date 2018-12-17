const moment = require('moment');
const timeseries = {};

timeseries.getMockTimeseries = (scr_id, start, end, interval) => {
    const newCollection = [];
    let i = 0;


    var s = moment(start);
    var e = moment(end);
    var dur = moment.duration(e.diff(s));
    var min = dur.asMinutes();
    var nodes = min/interval;
    var utc = moment.utc(s);

    while(i < nodes) {
        newCollection.push(
            {src_id: scr_id,
                timestamp: utc.format('x'),
                latitude: 11.3749184,
                longitude: 143.9662336,
                ir_thermo_temperature_filtered: 29.04,
                relative_humidity: 78.94,
                air_temp: 29.02,
                wind_speed_world_filtered: 8.76
            }
        );
        utc.add(interval,'m');
        i++;
    }
    return newCollection;
};

module.exports = timeseries;
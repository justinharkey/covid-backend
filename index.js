const cron = require('node-cron');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js')
require('dotenv').config();

cron.schedule('0 0 * * *', function() {
	console.log('------------------------------');

	const supabaseUrl = process.env.SUPABASE_HOSTNAME;
	const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

	const supabase = createClient(supabaseUrl, supabaseAnonKey);

	const parseCSVData = (unparsedData) => {
		console.log("parsing data");
		let arrData = [[]];
		// regex from https://gist.github.com/Jezternz/c8e9fafc2c114e079829974e3764db75
		const objPattern = new RegExp(
			'(\\,|\\r?\\n|\\r|^)(?:"([^"]*(?:""[^"]*)*)"|([^\\,\\r\\n]*))',
			"gi"
		);
		let arrMatches = null;
		while ((arrMatches = objPattern.exec(unparsedData))) {
			if (arrMatches[1].length && arrMatches[1] !== ",") arrData.push([]);
			arrData[arrData.length - 1].push(
				arrMatches[2]
					? arrMatches[2].replace(new RegExp('""', "g"), '"')
					: arrMatches[3]
			);
		}
		console.log("parseCSVData complete");
		return arrData;
	};

	const getFips = async () => {
		console.log("getFips");
		const { data, error } = await supabase.from("us_counties").select("fips");

		if (error) {
			console.log("getFips error:", error);
		}

		console.log("getFips complete");
		return data;
	};

	const insertData = async (cleanData, fips) => {
		console.log("inserting data");
		let insertData = [];

		for (let i = 1; i < cleanData.length; i++) {
			const fipsNumber = cleanData[i][3];
			// skip first row because of headers
			if (
				fipsNumber &&
				fips.find((x) => x.fips === parseInt(fipsNumber, 10))
			) {
				insertData.push({
					id: parseInt(
						`${cleanData[i][0]}${fipsNumber}`.replace(/-/g, ""),
						10
					),
					date: cleanData[i][0],
					fips: parseInt(fipsNumber, 10),
					cases: parseInt(cleanData[i][4], 10),
					deaths: parseInt(cleanData[i][5], 10),
				});
			}
		}

		const { data, error, count } = await supabase
			.from("us_counties_cases")
			.upsert(insertData);

		if (error) {
			console.log("inserting data error:", error);
		}

		console.log(`inserting data complete`);
	};

	const getData = async () => {
		console.log("getting data");
		const response = await fetch(
			`https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv`
		);
		const covidData = await response.text();
		const parsedData = await parseCSVData(covidData);
		const fips = await getFips();
		await insertData(parsedData, fips);
	};

	getData();

});
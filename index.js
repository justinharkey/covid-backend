const http = require('http');
const cron = require('node-cron');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js')
require('dotenv').config();

http.createServer(function(req, res) {
	res.writeHead(200, {'Content-Type': 'text/plain'});
	res.write('Server is running.');
	res.end();
}).listen(process.env.PORT || 8080)

const postMessageToDiscord = async (message) => {
	message = message || 'Hello World!';
  
	const discordUrl = process.env.DISCORD_NOTIFICATION_URL;
	const payload = JSON.stringify({ content: message });
  
	const params = {
		headers: {
			'Content-Type': 'application/json',
		},
		method: 'POST',
		body: payload,
		muteHttpExceptions: true,
	};
  
	await fetch(discordUrl, params);
}

cron.schedule('0 */6 * * *', function() {
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

		postMessageToDiscord("parseCSVData complete");
		console.log("parseCSVData complete");
		return arrData;
	};

	const getFips = async () => {
		console.log("getFips");
		const { data, error } = await supabase.from("us_counties").select("fips");

		if (error) {
			postMessageToDiscord(`getFips error: ${error}`);
			console.log("getFips error:", error);
		}

		if (data) {
			postMessageToDiscord("getFips complete");
			console.log("getFips complete");
			return data;
		}
		return null;
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

		const { data, error } = await supabase
			.from("us_counties_cases")
			.upsert(insertData);

		if (error) {
			postMessageToDiscord(`inserting data error: ${error}`);
			console.log("inserting data error:", error);
		}

		postMessageToDiscord("inserting data complete");
		console.log(`inserting data complete`);
	};

	const getData = async () => {
		postMessageToDiscord("beginning data update for covid.justinharkey.com");
		console.log("getting data");
		const response = await fetch(
			`https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv`
		);
		const covidData = await response.text();
		const parsedData = await parseCSVData(covidData);
		const fips = await getFips();
		if (fips) {
			await insertData(parsedData, fips);
		} else {
			postMessageToDiscord("fips failed, aborted insert");
			console.log(`fips failed, aborted insert`);
		}
	};

	getData();

});
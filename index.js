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

const fipsCache = {
	data: null,
	updatedAt: null
};

const dateColumn = 0;
const fipsColumn = 3;
const casesColumn = 4;
const deathsColumn = 5;

let statusMessages = [];

const postMessageToDiscord = async (message) => {
	if (!message) {
		return null;
	}
  
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

const parseCSVData = (unparsedData) => {
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

	statusMessages.push("✅ parseCSVData");
	return arrData;
};

const buildErrorMessage = (error) => {
	let errorMessage = "";
	if (error.code) {
		errorMessage += `Code: ${error.code} | `;
	}
	if (error.details) {
		errorMessage += `Details: ${error.details} | `;
	}
	if (error.message) {
		errorMessage += `Message: ${error.message} | `;
	}
	if (error.hint) {
		errorMessage += `Hint: ${error.hint} | `;
	}
	return errorMessage;
}

const buildDiscordMessage = (messages) => {
	let discordMessage = '';
	for (let i = 0; i < messages.length; i++) {
		discordMessage += messages[i];
		if (i !== messages.length - 1) {
			discordMessage += ' | ';
		}
	}
	return discordMessage;
}

cron.schedule('0 0 * * *', function() {
	const supabaseUrl = process.env.SUPABASE_HOSTNAME;
	const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

	const supabase = createClient(supabaseUrl, supabaseAnonKey);

	const getFips = async () => {
		const oneDayDuration = 60 * 60 * 24 * 1000;
		if (fipsCache.data && Date.now() - fipsCache.updatedAt <= oneDayDuration) {
			return fipsCache.data;
		}
		
		const { data, error } = await supabase.from("us_counties").select("fips");

		if (error) {
			statusMessages.push(`❌ getFips ${buildErrorMessage(error)}`);
		}

		if (data) {
			statusMessages.push("✅ getFips");

			fipsCache.data = data;
    		fipsCache.updatedAt = Date.now();

			return data;
		}

		return null;
	};

	const insertData = async (cleanData, fips) => {
		if (!fips) {
			statusMessages.push("❌ fips failed, aborted insert");
			return null;
		}

		let insertData = [];

		for (let i = 1; i < cleanData.length; i++) {
			const fipsNumber = cleanData[i][fipsColumn];
			// skip first row because of headers
			if (
				fipsNumber &&
				fips.find((x) => x.fips === parseInt(fipsNumber, 10))
			) {
				insertData.push({
					id: parseInt(
						`${cleanData[i][dateColumn]}${fipsNumber}`.replace(/-/g, ""),
						10
					),
					date: cleanData[i][dateColumn],
					fips: parseInt(fipsNumber, 10),
					cases: parseInt(cleanData[i][casesColumn], 10),
					deaths: parseInt(cleanData[i][deathsColumn], 10),
				});
			}
		}

		const { data, error } = await supabase
			.from("us_counties_cases")
			.upsert(insertData);

		if (error) {
			statusMessages.push(`❌ insertData ${buildErrorMessage(error)}`);
		} else {
			statusMessages.push("✅ insertData");
		}
	};

	const loadCovidDataFromCsv = async () => {
		statusMessages.push("beginning data upsert for covid.justinharkey.com");
		const response = await fetch(
			`https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv`
		);
		const covidData = await response.text();
		const parsedData = await parseCSVData(covidData);
		const fips = await getFips();
		await insertData(parsedData, fips);

		const discordMessage = buildDiscordMessage(statusMessages);
		postMessageToDiscord(discordMessage);
		statusMessages = [];
	};

	loadCovidDataFromCsv();

});
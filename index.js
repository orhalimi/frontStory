const fs = require("fs");
const { parse } = require("csv-parse");
const isBefore = require("date-fns/isBefore");
const isAfter = require("date-fns/isAfter");

function aggregate({ path, aggregateFields, dateFrom, dateTo }) {
  return new Promise((resolve, reject) => {
    const data = {};

    fs.createReadStream(path)
      .pipe(parse({ delimiter: ",", columns: true, cast: true }))
      .on("data", function (row) {
        const dateTime = new Date(row.data_date);
        const date = `${dateTime.getFullYear()}-${dateTime.getMonth()}-${dateTime.getDay()}`;

        if (
          (dateFrom && isBefore(new Date(date), new Date(dateFrom))) ||
          (dateAfter && isAfter(new Date(date), new Date(dateAfter)))
        ) {
          return;
        }

        const campaign = row.campaign_id;
        if (!data[date]) {
          data[date] = { campaign: { row } };
        } else if (!data[date][campaign]) {
          data[date][campaign] = row;
        } else {
          for (let key in row) {
            if (!aggregateFields.includes(key)) {
              continue;
            }
            data[date][campaign][key] += row[key];
          }
        }
      })
      .on("end", function () {
        resolve(data);
      })
      .on("error", function (error) {
        reject(error);
      });
  });
}

function generateReport(costAgg, revenueAgg) {
  const reports = [];
  for (const day in costAgg) {
    for (const campaignId in costAgg[day]) {
      const aggRow = {
        date: day,
        campaign_id: campaignId,
        campaign_name: costAgg[day][campaignId].campaign_name,
        total_revenue: revenueAgg[day][campaignId].revenue,
        total_cost: costAgg[day][campaignId].cost,
        total_profit:
          revenueAgg[day][campaignId].revenue - costAgg[day][campaignId].cost,
        total_clicks: costAgg[day][campaignId].clicks,
        total_roi:
          revenueAgg[day][campaignId].revenue /
            costAgg[day][campaignId].clicks -
          costAgg[day][campaignId].cost / costAgg[day][campaignId].clicks,
      };
      reports.push(aggRow);
    }
  }
  return reports;
}

async function run(dateFrom, dateTo) {
  const costDailytAgg = await aggregate({
    path: "./cost_1.csv",
    aggregateFields: ["clicks", "cost"],
    dateFrom,
    dateTo,
  });

  const revenueDailyAgg = await aggregate({
    path: "./revenue_1.csv",
    aggregateFields: ["revenue"],
    dateFrom,
    dateTo,
  });

  const reports = generateReport(costDailytAgg, revenueDailyAgg);
  console.log(reports);
}

run();

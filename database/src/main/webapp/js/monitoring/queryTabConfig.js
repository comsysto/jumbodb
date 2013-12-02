/**
 * Created with IntelliJ IDEA.
 * User: alica
 * Date: 11/25/13
 * Time: 4:11 PM
 * To change this template use File | Settings | File Templates.
 */
define(["dimple"], function () {
	var collectionDataColumnName = "Collection",
		xPositionOfLegend = "86%";

	function setBasicChartSettings(myChart, columnName, chartType) {
		myChart.setBounds(60, 30, "80%", 305);
		var x = myChart.addCategoryAxis("x", "Date");
		x.addOrderRule("Date");
		myChart.addMeasureAxis("y", columnName);
		myChart.addSeries(collectionDataColumnName, chartType);

	}

	function addLegendTitle(svg) {
		// This block simply adds the legend title. I put it into a d3 data
		// object to split it onto 2 lines.  This technique works with any
		// number of lines, it isn't dimple specific.
		svg.selectAll("title_text")
			.data(["Click legend to", "show/hide collections:"])
			.enter()
			.append("text")
			.attr("x", xPositionOfLegend)
			.attr("y", function (d, i) {
				return 30 + i * 14;
			})
			.style("font-family", "sans-serif")
			.style("font-size", "10px")
			.style("color", "Black")
			.text(function (d) {
				return d;
			});
	}

	function makeLegendsSelectable(myChart, svg, data, myLegend) {
		// This is a critical step.  By doing this we orphan the legend. This
		// means it will not respond to graph updates.  Without this the legend
		// will redraw when the chart refreshes removing the unchecked item and
		// also dropping the events we define below.
		myChart.legends = [];
		addLegendTitle(svg);
		// Get a unique list of Owner values to use when filtering
		var filterValues = dimple.getUniqueValues(data, collectionDataColumnName);
		// Get all the rectangles from our now orphaned legend
		myLegend.shapes.selectAll("rect")
			// Add a click event to each rectangle
			.on("click", function (e) {
				// This indicates whether the item is already visible or not
				var hide = false;
				var newFilters = [];
				// If the filters contain the clicked shape hide it
				filterValues.forEach(function (f) {
					if (f === e.aggField.slice(-1)[0]) {
						hide = true;
					} else {
						newFilters.push(f);
					}
				});
				// Hide the shape or show it
				if (hide) {
					d3.select(this).style("opacity", 0.2);
				} else {
					newFilters.push(e.aggField.slice(-1)[0]);
					d3.select(this).style("opacity", 0.8);
				}
				// Update the filters
				filterValues = newFilters;
				// Filter the data
				myChart.data = dimple.filterData(data, collectionDataColumnName, filterValues);
				// Passing a duration parameter makes the chart animate. Without
				// it there is no transition
				myChart.draw(600, false);

			});
	}

	function addChart(htmlSelectorForChartDiv, columnName, chartType) {
		$(htmlSelectorForChartDiv).html("");
		var svg = dimple.newSvg(htmlSelectorForChartDiv, $(htmlSelectorForChartDiv).width() - 20, $(htmlSelectorForChartDiv).height());
		d3.tsv("js/monitoring/example_data.tsv", function (data) {

			var myChart = new dimple.chart(svg, data);
			setBasicChartSettings(myChart, columnName, chartType);
			var myLegend = myChart.addLegend(xPositionOfLegend, "10%", 20, 500, "left");
			myChart.draw();
			makeLegendsSelectable(myChart, svg, data, myLegend);
		});
	}

	return {
		template: "partials/monitoring/queryMonitoring.html",
		title: "Query",
		active: false,
		select: function (){
			addChart("div#firstChart", "Queries",  dimple.plot.bubble);
			addChart("div#secondChart", "SizeOfReturnedData", dimple.plot.bubble);
			addChart("div#thirdChart", "ResponseTimes(ms)", dimple.plot.line);
		}
	}
});



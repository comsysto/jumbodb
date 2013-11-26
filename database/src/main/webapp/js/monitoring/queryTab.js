/**
 * Created with IntelliJ IDEA.
 * User: alica
 * Date: 11/25/13
 * Time: 4:11 PM
 * To change this template use File | Settings | File Templates.
 */
define([], function () {

	return {
		template: "partials/monitoring/queryMonitoring.html",
		title: "Query",
		active: false,
		select: function (){
			$("div#firstChart").html("");
			var svg = dimple.newSvg("div#firstChart", $("div#firstChart").width(), $("div#firstChart").height());
			var data = [
				{ "Word":"Hello", "Awesomeness":2000 },
				{ "Word":"World", "Awesomeness":3000 }
			];
			var chart = new dimple.chart(svg, data);
			chart.addCategoryAxis("x", "Word");
			chart.addMeasureAxis("y", "Awesomeness");
			chart.addSeries(null, dimple.plot.bar);
			chart.draw();
		}
	}
});


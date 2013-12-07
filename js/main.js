(function($) {
	$.fn.displayIn = function(container) {
		var $container = $(container),
			original = $container.html(),
			selector = this.selector;

		this.parent()
		.mouseover(function() { $container.html($(selector, this).html()); })
		.mouseout(function() { $container.html(original); });
	};
})(jQuery);

jQuery(function() {
	jQuery('.query').displayIn('#query code');
});

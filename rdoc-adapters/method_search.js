var search_box = document.getElementById('search');
var search_timeout;
var search_value = search_box.value;

function performSearch(e) {
  if (e) {
    e.preventDefault();
  }
  if (search.value === search_value) {
    return;
  }
  search_value = search_box.value;

	if (search_value == '') {
    document.querySelectorAll("#index-entries li.hide").forEach(elem => {
      elem.classList.remove('hide');
    });
	} else {
    document.querySelectorAll("#index-entries span.method_name").forEach(elem => {
      var value = elem.getAttribute('value');
      var li_classes = elem.parentElement.parentElement.classList;
      if (value && value.includes(search_value)) {
        li_classes.remove('hide');
      } else {
        li_classes.add('hide');
      }
    });
  }
}

document.getElementById('search_form').onsubmit = performSearch;

search.oninput = function(e) {
  if (search_timeout) {
    clearTimeout(search_timeout);
  }
  search_timeout = setTimeout(performSearch, 300);
};

document.getElementById('clear_button').onclick = function(e) {
  e.stopPropagation();
  search_box.value = '';
  performSearch();
  search_box.focus();
};

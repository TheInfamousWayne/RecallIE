function extract() {
    var textToAnalyse = document.getElementById('texttoanalyse').value;//.innerText;
    console.log(textToAnalyse);
    $.ajax({
        url: 'result',
        data: {
            'texttoanalyse': textToAnalyse,
        },
        dataType: 'json',
        success: function(data){
            console.log(data);
            document.getElementById('response').innerHTML = data['entity_1_group'] +"<br><br>"+ data['entity_2_group'];
        }
        
    })
}

function store(e) {
    value = e.innerHTML;
    localStorage.setItem("li", value);
    console.log(localStorage.getItem("li"));
    if (e.id == 'm') {
        window.open('/personal/','_self');
    }
    else {
        window.open('/links/','_self');   
    }
}

function default_value() {
    localStorage.setItem("li", "");
    localStorage.setItem("entity", "");
}
    



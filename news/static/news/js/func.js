function customize() {
    button_load();
}

function button_load() {
    value = localStorage.getItem("entity");
    localStorage.setItem("entity", "");
    console.log(value);
    button_string = '';
    if (value == "") {
        button_string = '<a class="btn btn-dark" role="button" href="/">Back</a>';
    } else if (value=='headline') {
        button_string = '<a class="btn btn-dark" role="button" href="/">Home</a>';
    }
    else {
        button_string = '<a class="btn btn-dark" role="button" href="/news/" >Back</a>\
                        <a class="btn btn-dark" role="button" href="/">Home</a>\
                        <a class="btn btn-warning" role="button" data-toggle="modal" data-target="#recall_df">See Last Recall Output</a>';
    }
    document.getElementById('back_buttons').innerHTML = button_string;
}

function refresh() {
    document.getElementById('response').innerHTML = "";
    document.getElementById('response2').innerHTML = "";
    document.getElementById('debugging').innerHTML = "";
}


function get_recall(link) {
    document.getElementById('modal_body').innerHTML = '<br><br><centre> <div class="dot-carousel"> </div> </centre>' ;
    localStorage.setItem("entity", "headline");
    console.log(link);
    $.ajax({
        url: 'result',
        data: {
            'link': link,
        },
        dataType: 'json',
        success: function(data){
            console.log(data['news']);
            document.getElementById('modal_body').innerHTML = data['news'];
        }
        
    })
}


function get_headlines(e) {
    value = e.innerHTML;
    localStorage.setItem("entity", value);
    console.log(value); 
    $.ajax({
        url: 'headlines',
        data: {
            'entity': value,
        },
        dataType: 'json',
        success: function(data){
            console.log(data['headlines']);
            headlines_html = '<ul>\n';
            for (var headline in data['headlines']) {
                link = data['headlines'][headline];
                console.log(headline);
                headlines_html +=  '<li data-toggle="modal" data-target="#recall_df" onclick="get_recall(\'$link\')">$headline</li>\n'.replace('$headline',headline).replace('$link',link);
            }
            headlines_html += '</ul>\n';
            document.getElementById('entity_list').innerHTML = headlines_html;
            console.log(headlines_html);
        }
        
    });
    button_load();
}
function customize() {
    button_load();
}

function button_load() {
    value = localStorage.getItem("entity");
    localStorage.setItem("parent_entity",value);
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
    localStorage.setItem("entity", '');
    entity = localStorage.getItem("parent_entity");
    localStorage.setItem("link", link);
    console.log(link);
    $.ajax({
        url: 'recall',
        data: {
            'link': link,
            'query': entity
        },
        dataType: 'json',
        success: function(data){
            console.log(data['news_df']);
            document.getElementById('modal_body').innerHTML = data['news_df'];
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


function updateDF(e) {
    value = e;
    document.getElementById('threshold_conf').innerHTML = value;
    console.log(value);
    //document.getElementById('modal_body').innerHTML = '<br><br><centre> <div class="dot-carousel"> </div> </centre>' ;
    link = localStorage.getItem("link");
    entity = localStorage.getItem("parent_entity");
    $.ajax({
        url: 'update',
        data: {
            'link': link,
            'query': entity,
            'confidence': value
        },
        dataType: 'json',
        success: function(data){
            console.log(data['news_df']);
            document.getElementById('modal_body').innerHTML = data['news_df'];
        }
        
    })
    
}


function updateThresholdDisplay(e) {
    document.getElementById('threshold_conf').innerHTML = e;
}
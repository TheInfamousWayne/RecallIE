
function button_load() {
    document.getElementById('response').innerHTML = '<br><br><centre> <div class="dot-carousel"> </div> </centre>' ;
}

function refresh() {
    document.getElementById('response').innerHTML = "";
    document.getElementById('response2').innerHTML = "";
    document.getElementById('debugging').innerHTML = "";
}



function check_passed() {
    console.log(localStorage.getItem("li"));
    document.getElementById('texttoanalyse').value = localStorage.getItem("li");
    if (localStorage.getItem("li") != "") {
        extract();
    }
}


function extract() {
    refresh();
    button_load();
    var textToAnalyse = document.getElementById('texttoanalyse').value;//.innerText;
    console.log(textToAnalyse);
    $.ajax({
        url: 'result',
        data: {
            'texttoanalyse': textToAnalyse,
        },
        dataType: 'json',
        success: function(data){
            console.log(data['debug']);
            if (data['main_df']) {
                document.getElementById('response').innerHTML = "<br><br><h5>Main Table</h5>" + data['main_df'] + "<br><br><br><br>";
                document.getElementById('response2').innerHTML = "<br><br><h5>Other Table</h5>" + data['other_df'] + "<br><br><br><br>";
                document.getElementById('debugging').innerHTML = "<br><br><h5>For Debugging</h5>" + data['debug'] + "<br><br><br><br><br><br><br><br>";
            }
            else {
                console.log(data['error']);
                document.getElementById('response').innerHTML = "<br><br><h5>Error!</h5><p>Could not extract relation for this entity.<br> Please try another one.</p>";
                document.getElementById('response2').innerHTML = "<br><br><h5>Error Message</h5>" + data['error'] + "<br><br><br><br>";
            }
        }
        
    })
}


function eval(e) {
    document.getElementById('texttoanalyse').value = e.innerHTML;
    extract();
}
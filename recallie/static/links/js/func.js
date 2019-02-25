function suggestions() {
    var searchContent = document.getElementById('searchbar').value;//.innerText;
    console.log(searchContent);
    $.ajax({
        url: 'suggest',
        data: {
            'searchbar': searchContent,
        },
        dataType: 'json',
        success: function(data){
            console.log(data);
            var text = ""
            var i;
            for (i = 0; i < data['suggestions'].length; i++) {
              text +=  "<option value='" + data['suggestions'][i] + "'>" + data['suggestions'][i] + "</option>";
            }
            
            document.getElementById('suggestions-menu').innerHTML = text;
        }
        
    })
}


function check_passed() {
    console.log(localStorage.getItem("li"));
    document.getElementById('searchbar').value = localStorage.getItem("li");
    if (localStorage.getItem("li") != "") {
        extract();
    }
}

function button_load() {
    document.getElementById('response').innerHTML = '<br><br><centre> <div class="dot-carousel"> </div> </centre>' ;
}

function refresh() {
    document.getElementById('response').innerHTML = "";
    document.getElementById('response2').innerHTML = "";
    document.getElementById('debugging').innerHTML = "";
}


function extract() {
    refresh();
    button_load();
    var searchContent = document.getElementById('searchbar').value;//.innerText;
    console.log(searchContent);
    $.ajax({
        url: 'result',
        data: {
            'searchbar': searchContent,
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
var last_id=0;
var live_events_busy=0;
function get_live_events() {
if (live_events_busy) return;
live_events_busy=1;
$.ajax('/ui/ajax/live', {
data:{last_id:last_id}, 
dataType: 'json',
beforeSend:function(){$('#loading').addClass('loading');},
complete:function(){$('#loading').removeClass('loading'); live_events_busy=0},
success:function(data){if (data.content) $('#live_events').prepend(data.content); last_id=data.last_id;}
}
);
}

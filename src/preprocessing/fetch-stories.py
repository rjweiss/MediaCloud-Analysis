import os, sys, logging, json, datetime, time
import ConfigParser
import mediacloud

STORIES_PER_PAGE = 1000

logging.basicConfig(filename='fetcher.log',level=logging.DEBUG)
log = logging.getLogger('fetcher')
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
config = ConfigParser.ConfigParser()
config.read('mc-client.config')

mc = mediacloud.api.MediaCloud(config.get('api', 'key'))

def publishDateRange(d1, d2):
	return u'+publish_date: [{0} TO {1}]'.format(zi_time(d1), zi_time(d2))

# ISO Date Format w/ time zeroed
def zi_time(d):
	return datetime.datetime.combine(d, datetime.time.min).isoformat() + "Z"

def date_range(start, end):
    r = (end+datetime.timedelta(days=1)-start).days
    return [start+datetime.timedelta(days=i) for i in range(r)]
 
start = datetime.date(2012,8,1)
end = datetime.date(2012,11,10)
date_list = date_range(start, end)

mc_query = u'*'
mc_date_filter = publishDateRange(start, end) + 'AND +media_sets_id:1'

sentences_res = mc.sentenceList(mc_query, mc_date_filter, 0, 0)
total_number_sentences = sentences_res['response']['numFound']

print 'Date range = {start} to {end}'.format(start=start, end=end)
print 'Query string = {}'.format(mc_query)
print 'Total number of sentences = {}'.format(total_number_sentences)

db = mediacloud.storage.MongoStoryDatabase('election')

for i in xrange(len(date_list)-1):
    log.info('  processing date ' + str(date_list[i]))
    print 'Processing date ' + str(date_list[i])
    mc_date_filter = publishDateRange(date_list[i], date_list[i+1]) + 'AND +media_sets_id:1'
    more_stories = True
    last_processed_stories_id = 0
    while more_stories:
        try:
            stories = mc.storyList(mc_query, mc_date_filter, last_processed_stories_id, STORIES_PER_PAGE)
            if len(stories)>0:
                for story in stories:
                    saved = db.addStory(story)
                    if saved:
                        log.info('  saved '+str(story['processed_stories_id']))
                    else:
                        log.info('  skipped '+str(story['processed_stories_id']))
                    last_processed_stories_id = stories[len(stories)-1]['processed_stories_id']
                    more_stories = True
            else:
                more_stories = False
        except Exception as e:
            #probably a 404, so sleep and then just try again
            log.info('  '+str(e))
            time.sleep(1)


print "Finished."	
	


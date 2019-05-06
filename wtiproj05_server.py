import cherrypy
import json
from wtiproj05_api_logic import ApiLogic


@cherrypy.expose
@cherrypy.tools.json_out()
class Ratings(object):
    @cherrypy.tools.accept(media='application/json')
    def GET(self):
        ratings = api_logic.list_rating()
        return ratings

    def DELETE(self):
        api_logic.delete_ratings()
        return 'DELETED'


@cherrypy.expose
@cherrypy.tools.json_out()
class Rating(object):
    @cherrypy.tools.accept(media="application/json'")
    def POST(self):
        cl = cherrypy.request.headers['Content-Length']
        rawbody = cherrypy.request.body.read(int(cl))
        result = json.loads(rawbody)
        api_logic.add_rating(result)
        return result


@cherrypy.expose
@cherrypy.tools.json_out()
class AvgAll(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self, args):
        if args == 'all-users':
            avg_genre_all_ratings, _ = api_logic.compute_avg_genre_ratings()
            return avg_genre_all_ratings
        else:
            avg_genre_user_ratings, _ = api_logic.compute_avg_genre_ratings_for_user(int(args))
            return avg_genre_user_ratings


@cherrypy.expose
@cherrypy.tools.json_out()
class UserProfile(object):
    @cherrypy.tools.accept(media="application/json")
    def GET(self, arg):
        return api_logic.compute_user_profile(int(arg))


if __name__ == '__main__':
    api_logic = ApiLogic(port=6379, file_records=10)

    conf = {
        '/':{
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
        'global': {
            'engine.autoreload.on': False
        }
    }

    cherrypy.config.update({'server.socket_port': 6161})
    cherrypy.tree.mount(Ratings(), '/ratings', conf)
    cherrypy.tree.mount(Rating(), "/rating", conf)
    cherrypy.tree.mount(AvgAll(), "/avg-genre-ratings", conf)
    cherrypy.tree.mount(UserProfile(), "/user-profile", conf)

    cherrypy.engine.start()
    cherrypy.engine.block()

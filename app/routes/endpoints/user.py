from datetime import datetime
import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User, get_residents, get_residents_contacts, get_community_managers, get_telegram_pin, get_last_activity, get_users_memberships
from app.models.event import Event
from app.models.item import Item
from app.helpers.mobile import send_mobile_message



def routes():
    return [
        Route('/user/{id:int}/info', user_info, methods = [ 'POST' ]),
        Route('/user/update', user_update, methods = [ 'POST' ]),
        Route('/user/summary', user_summary, methods = [ 'POST' ]),
        Route('/user/contacts', user_contacts, methods = [ 'POST' ]),
        Route('/user/recommendations', user_recommendations, methods = [ 'POST' ]),
        Route('/user/suggestions', user_suggestions, methods = [ 'POST' ]),
        Route('/user/suggestions/stats', user_suggestions_stats, methods = [ 'POST' ]),
        Route('/user/search', user_search, methods = [ 'POST' ]),
        Route('/user/contact/add', user_add_contact, methods = [ 'POST' ]),
        Route('/user/contact/del', user_del_contact, methods = [ 'POST' ]),
        Route('/user/event/add', user_add_event, methods = [ 'POST' ]),
        Route('/user/event/del', user_del_event, methods = [ 'POST' ]),
        Route('/user/thumbsup', user_thumbs_up, methods = [ 'POST' ]),
        Route('/user/thumbsoff', user_thumbs_off, methods = [ 'POST' ]),

        Route('/user/{id:int}/helpful', user_helpful, methods = [ 'POST' ]),

        Route('/user/telegram/get/pin', save_telegram_pin, methods = [ 'POST' ]),

        Route('/m/user/search', moderator_user_search, methods = [ 'POST' ]),
        Route('/m/user/for/select', moderator_user_for_select, methods = [ 'POST' ]),
        Route('/m/user/update', moderator_user_update, methods = [ 'POST' ]),
        Route('/m/user/create', moderator_user_create, methods = [ 'POST' ]),

        Route('/new/user/residents', user_residents, methods = [ 'POST' ]),
        Route('/new/user/{id:int}/info', new_user_info, methods = [ 'POST' ]),
        Route('/new/user/update', new_user_update, methods = [ 'POST' ]),

        Route('/new/m/user/update', new_moderator_user_update, methods = [ 'POST' ]),
        Route('/new/m/user/create', new_moderator_user_create, methods = [ 'POST' ]),
        Route('/new/m/user/event/confirm', new_moderator_user_confirm_event, methods = [ 'POST' ]),

        Route('/ma/user/search', manager_user_search, methods = [ 'POST' ]),
        Route('/ma/user/update', manager_user_update, methods = [ 'POST' ]),
        Route('/ma/user/membership/stage/update/{field:str}', manager_user_membership_stage_update, methods = [ 'POST' ]),
        Route('/ma/user/membership/rating/update/{field:str}', manager_user_membership_rating_update, methods = [ 'POST' ]),
        Route('/ma/user/create', manager_user_create, methods = [ 'POST' ]),

        Route('/ma/user/event/confirm', manager_user_confirm_event, methods = [ 'POST' ]),
        Route('/ma/user/event/add', manager_user_add_event, methods = [ 'POST' ]),
        Route('/ma/user/event/del', manager_user_del_event, methods = [ 'POST' ]),
        Route('/ma/user/event/audit', manager_user_audit_event, methods = [ 'POST' ]),
    ]



MODELS = {
	'user_info': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_helpful': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_update': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
		'company': {
			'required': True,
			'type': 'str',
		},
		'position': {
			'required': True,
			'type': 'str',
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
	},
	'user_search': {
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
        'reverse': {
			'required': True,
			'type': 'bool',
            'default': False,
		},
        'target': {
            'required': True,
			'type': 'str',
            'null': True,
            'values': [ 'tags', 'interests' ],
        },
	},
	'user_suggestions': {
		'id': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'filter': {
			'required': True,
			'type': 'str',
            'values': [ 'tags', 'interests' ],
            'null': True,
		},
		'today': {
			'required': True,
			'type': 'bool',
            'default': False,
		},
        'from_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'null': True,
        }
	},
	'user_add_contact': {
		'contact_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_del_contact': {
		'contact_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_add_event': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_del_event': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_thumbs_up': {
		'item_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_thumbs_off': {
		'item_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
    # moderator
	'moderator_user_search': {
		'text': {
			'required': True,
			'type': 'str',
		},
        'applicant': {
            'required': True,
            'type': 'bool',
            'default': False,
            'null': True,
        },
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
	'moderator_user_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'client', 'guest', 'manager', 'moderator', 'editor' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
	},
	'moderator_user_create': {
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'admin', 'client', 'guest', 'manager', 'moderator', 'editor', 'chief', 'community manager', 'tester', 'speaker' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
	},
    'user_residents': {
        'users_ids': {
            'required': False,
			'type': 'int',
            'list': True,
            'null': True,
            'default': None,
        },
    },
    # new
	'new_user_info': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'new_user_update': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'annual': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'employees': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'tags': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'interests': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'city': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'hobby': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'detail': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
	},
    # new
	'new_moderator_user_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
        'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'admin', 'client', 'guest', 'manager', 'moderator', 'editor', 'chief', 'community manager', 'tester', 'speaker' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
	},
	'new_moderator_user_create': {
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'client', 'guest', 'manager', 'chief', 'community manager' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
	},
    'user_suggestions_stats': {
		'date_offset': {
			'required': True,
			'type': 'int',
		},
    },
    'new_moderator_user_confirm_event': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    # manager
	'manager_user_search': {
		'text': {
			'required': True,
			'type': 'str',
		},
		'ids': {
			'required': True,
			'type': 'int',
            'list': True,
            'null': True,
		},
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'null': True,
        },
        'filter': {
            'required': True,
            'type': 'str',
            'list': True,
            'null': True,
        },
        'ignore_community_manager': {
            'required': True,
            'type': 'bool',
            'default': False,
        },
	},
	'manager_user_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
        'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
	},
    'manager_user_membership_stage_update': {
		'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'stage_id': {
            'required': True,
			'type': 'int',
            'value_min': 1,
            'value_max': 6,
        },
        'field': {
            'required': True,
			'type': 'str',
            'values': [ 'comment', 'time_control', 'rejection', 'active' ],
        },
        'value': {
            'required': True,
			'type': 'str',
            'null': True,
        },
    },
    'manager_user_membership_rating_update': {
		'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'field': {
            'required': True,
			'type': 'str',
            'values': [ 'comment', 'rating' ],
        },
        'value': {
            'required': True,
			'type': 'str',
            'null': True,
        },
    },
	'manager_user_create': {
		'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
	},
    'manager_user_confirm_event': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_add_event': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_del_event': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_audit_event': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'audit': {
			'required': True,
			'type': 'bool',
		},
    },
}



################################################################
async def user_info(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_info']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                result = user.show()
                result.update({
                    'contacts_cache': False,
                    'allow_contact': False,
                })
                contacts = await request.user.get_contacts()
                for contact in contacts:
                    if contact['id'] == user.id:
                        result['contacts_cache'] = True
                        break
                result['allow_contact'] = await request.user.check_access(user)
                ### remove data for roles
                roles = set(request.user.roles)
                roles.discard('applicant')
                roles.discard('guest')
                if not roles and request.user.id != result['id']:
                    result['company'] = ''
                    result['position'] = ''
                    result['link_telegram'] = ''
                return OrjsonResponse(result)
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_update(request):
    if request.user.id:
        if validate(request.params, MODELS['user_update']):
            await request.user.update(**request.params)
            dispatch('user_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_summary(request):
    if request.user.id:
        result = await request.user.get_summary()
        result['amounts_messages'] = await request.user.get_unread_messages_amount()
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_contacts(request):
    if request.user.id:
        result = await request.user.get_contacts()
        ### remove data for roles
        roles = set(request.user.roles)
        roles.discard('applicant')
        roles.discard('guest')
        if not roles:
            for item in result:
                if request.user.id != item['id']:
                    item['company'] = ''
                    item['position'] = ''
                    item['link_telegram'] = ''
        return OrjsonResponse({ 'contacts': result })
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_recommendations(request):
    if request.user.id:
        result = await request.user.get_recommendations()
        ### remove data for roles
        roles = set(request.user.roles)
        roles.discard('applicant')
        roles.discard('guest')
        if not roles:
            for item in result['tags']:
                if request.user.id != item['id']:
                    item['company'] = ''
                    item['position'] = ''
                    item['link_telegram'] = ''
            for item in result['interests']:
                if request.user.id != item['id']:
                    item['company'] = ''
                    item['position'] = ''
                    item['link_telegram'] = ''
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_suggestions(request):
    if request.user.id:
        if validate(request.params, MODELS['user_suggestions']):
            result = await request.user.get_suggestions(
                id = request.params['id'],
                filter = request.params['filter'],
                today_offset = None,
                from_id = request.params['from_id'],
            )
            ### remove data for roles
            roles = set(request.user.roles)
            roles.discard('applicant')
            roles.discard('guest')
            if not roles:
                for item in result:
                    if request.user.id != item['id']:
                        item['company'] = ''
                        item['position'] = ''
                        item['link_telegram'] = ''
            return OrjsonResponse({ 'suggestions': result })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_suggestions_stats(request):
    if request.user.id:
        if validate(request.params, MODELS['user_suggestions_stats']):
            result = await request.user.get_suggestions(
                id = None,
                filter = None,
                today_offset = request.params['date_offset'],
            )
            stats = {
                'bid': 0,
                'ask': 0,
            }
            for item in result:
                if item['offer'] == 'bid':
                    stats['bid'] += 1
                else:
                    stats['ask'] += 1
            return OrjsonResponse(stats)
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_search(request):
    if request.user.id:
        if validate(request.params, MODELS['user_search']):
            result = await User.search(
                text = request.params['text'],
                reverse = request.params['reverse'],
                offset = 0,
                limit = 50,
                target = request.params['target'],
            )
            contacts = await request.user.get_contacts()
            allow_contacts = {}
            if result:
                allow_contacts = await request.user.check_multiple_access([ item for item in result if item.id != request.user.id ])
            ### remove data for roles
            roles = set(request.user.roles)
            roles.discard('applicant')
            roles.discard('guest')
            persons = []
            for item in result:
                if item.id != request.user.id:
                    temp = item.show()
                    if not roles:
                        temp['company'] = ''
                        temp['position'] = ''
                        temp['link_telegram'] = ''
                    persons.append(temp)
            return OrjsonResponse({
                'persons': persons,
                'contacts_cache': { str(contact['id']): True for contact in contacts if contact['type'] == 'person' },
                'allow_contacts': allow_contacts,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_add_contact(request):
    if request.user.id:
        if validate(request.params, MODELS['user_add_contact']):
            user = User()
            await user.set(id = request.params['contact_id'])
            if request.user.id == user.id:
                return err(400, 'Неверный запрос')
            if user.id:
                access = await request.user.check_access(user)
                if access:
                    await request.user.add_contact(user.id)
                    dispatch('user_add_contact', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Контакт не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_del_contact(request):
    if request.user.id:
        if validate(request.params, MODELS['user_del_contact']):
            user = User()
            await user.set(id = request.params['contact_id'])
            if user.id:
                await request.user.del_contact(user.id)
                dispatch('user_del_contact', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Контакт не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_add_event(request):
    if request.user.id:
        if validate(request.params, MODELS['user_add_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                await request.user.add_event(event.id)
                dispatch('user_add_event', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_del_event(request):
    if request.user.id:
        if validate(request.params, MODELS['user_del_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                await request.user.del_event(event.id)
                dispatch('user_del_event', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_thumbs_up(request):
    if request.user.id:
        if validate(request.params, MODELS['user_thumbs_up']):
            item = Item()
            await item.set(id = request.params['item_id'])
            if item.id:
                await request.user.thumbsup(item.id)
                dispatch('user_thumbs_up', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Объект не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_thumbs_off(request):
    if request.user.id:
        if validate(request.params, MODELS['user_thumbs_off']):
            item = Item()
            await item.set(id = request.params['item_id'])
            if item.id:
                await request.user.thumbsoff(item.id)
                dispatch('user_thumbs_off', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Объект не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_user_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_user_search']):
            (result, amount) = await User.search(
                text = request.params['text'],
                active_only = False,
                offset = (request.params['page'] - 1) * 10,
                limit = 10,
                count = True,
                applicant = request.params['applicant'] if request.params['applicant'] is not None else False,
            )
            community_managers = await get_community_managers()
            users_ids = [ user.id for user in result ]
            activity = await get_last_activity(users_ids = users_ids)
            users = []
            for item in result:
                user_activity = { 'time_last_activity': activity[str(item.id)] if str(item.id) in activity else None }
                users.append(item.dump() | user_activity)
            return OrjsonResponse({
                'users': users,
                'amount': amount,
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
    


################################################################
async def moderator_user_for_select(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        (result, amount) = await User.for_select()
        return OrjsonResponse({
            'users': [ { 'id': user['id'], 'name': user['name'] } for user in result ],
            'amount': amount,
        })
    else:
        return err(403, 'Нет доступа')




################################################################
async def moderator_user_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_user_update']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                temp = User()
                if await temp.find(email = request.params['email']):
                    if temp.id != user.id:
                        return err(400, 'Email уже зарегистрирован')
                temp = User()
                if await temp.find(phone = request.params['phone']):
                    if temp.id != user.id:
                        return err(400, 'Телефон уже зарегистрирован')
                await user.update(**request.params)
                dispatch('user_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_user_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_user_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = request.params['company'],
                position = request.params['position'],
                password = request.params['password'],
                roles = request.params['roles'],
                active = request.params['active'],
                detail = request.params['detail'],
                status = request.params['status'],
                tags = request.params['tags'],
                interests = request.params['interests'],
            )
            dispatch('user_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_residents(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_residents']):
            result = await get_residents(users_ids = request.params['users_ids'] if 'users_ids' in request.params and request.params['users_ids'] else None)
            contacts = await get_residents_contacts(
                user_id = request.user.id,
                user_status = request.user.status,
                contacts_ids = [ item.id for item in result ]
            )
            ### remove data for roles
            roles = set(request.user.roles)
            roles.discard('applicant')
            roles.discard('guest')
            residents = []
            for item in result:
                temp = item.show()
                if not roles and request.user.id != temp['id']:
                    temp['company'] = ''
                    temp['position'] = ''
                    temp['link_telegram'] = ''
                residents.append(temp)
            return OrjsonResponse({
                'residents': residents,
                'contacts': contacts,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_user_info(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_info']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                result = {}
                if user.id == request.user.id:
                    result = user.dshow()
                    result.update({ 
                        'contact': False,
                        'allow_contact': False
                    })
                else:
                    result = user.show()
                    contact = await get_residents_contacts(
                        user_id = request.user.id,
                        user_status = request.user.status,
                        contacts_ids = [ user.id ]
                    )
                    result.update(contact[str(user.id)])
                ### remove data for roles
                roles = set(request.user.roles)
                roles.discard('applicant')
                roles.discard('guest')
                if not roles and request.user.id != result['id']:
                    result['company'] = ''
                    result['position'] = ''
                    result['link_telegram'] = ''
                return OrjsonResponse(result)
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_user_update(request):
    if request.user.id:
        if validate(request.params, MODELS['new_user_update']):
            await request.user.update(**request.params)
            dispatch('user_update', request)
            user = User()
            await user.set(id = request.user.id)
            result = user.dshow()
            result.update({ 
                'contact': False,
                'allow_contact': False
            })
            return OrjsonResponse(result)
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_moderator_user_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['new_moderator_user_update']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                temp = User()
                if await temp.find(email = request.params['email']):
                    if temp.id != user.id:
                        return err(400, 'Email уже зарегистрирован')
                temp = User()
                if await temp.find(phone = request.params['phone']):
                    if temp.id != user.id:
                        return err(400, 'Телефон уже зарегистрирован')
                if 'admin' in request.params['roles']:
                    if user.id not in { 8000, 10004 }:
                        return err(400, 'Неверный запрос')
                await user.update(**request.params)
                dispatch('user_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_moderator_user_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['new_moderator_user_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            if 'admin' in request.params['roles']:
                if user.id not in { 8000, 10004 }:
                    return err(400, 'Неверный запрос')
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = request.params['company'],
                position = request.params['position'],
                catalog = request.params['catalog'],
                password = request.params['password'],
                roles = request.params['roles'],
                active = request.params['active'],
                detail = request.params['detail'],
                status = request.params['status'],
                city = request.params['city'],
                hobby = request.params['hobby'],
                tags = request.params['tags'],
                interests = request.params['interests'],
                annual = request.params['annual'],
                annual_privacy = request.params['annual_privacy'],
                employees = request.params['employees'],
                employees_privacy = request.params['employees_privacy'],
                birthdate = request.params['birthdate'],
                birthdate_privacy = request.params['birthdate_privacy'],
                experience = request.params['experience'],
                community_manager_id = request.params['community_manager_id'],
            )
            dispatch('user_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_moderator_user_confirm_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['new_moderator_user_confirm_event']):
            user = User()
            await user.set(id = request.params['user_id'])
            if user.id:
                event = Event()
                await event.set(id = request.params['event_id'])
                if event.id:
                    await user.confirm_event(event_id = event.id)
                    dispatch('user_update', request)
                    return OrjsonResponse({
                        'event_id': event.id,
                        'user_id': user.id,
                        'confirmation': True,
                    })
                else:
                    return err(404, 'Событие не найдено')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_helpful(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_helpful']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                answers = await user.get_helpful_answers()
                return OrjsonResponse({
                    'answers': answers
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def save_telegram_pin(request):
    if request.user.id and request.user.active is True:
        pin = await get_telegram_pin(request.user)
        link = 'https://t.me/GermesClubBot?start=' + pin
        if request.user.phone:
            send_mobile_message(
                request.api.stream_mobile,
                request.user.phone,
                'Ссылка для привязки Telegram к Germes: ' + link,
            )
        return OrjsonResponse({
            'pin': pin,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_search']):
            community_manager_id = None
            if not request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
                if not request.params['ignore_community_manager']:
                    community_manager_id = request.user.id
            (result, amount) = await User.client_search(
                text = request.params['text'],
                ids = request.params['ids'],
                community_manager_id = community_manager_id,
                active_only = False,
                offset = (request.params['page'] - 1) * 15 if request.params['page'] else None,
                limit = 15 if request.params['page'] else None,
                count = True,
            )
            community_managers = await get_community_managers()
            users_ids = [ user.id for user in result ]
            activity = await get_last_activity(users_ids = users_ids)
            memberships = await get_users_memberships(users_ids)
            users = []
            membership_template = {
                'rating': None,
                'stage': 1,
                'semaphore': [
                    {
                        'id': 1,
                        'name': 'Оценка менеджера',
                        'rating': None,
                        'data': { 'comment': None, },
                    },
                    {
                        'id': 2,
                        'name': 'Участие в опросах',
                        'rating': None,
                        'data': { 'value': 0, },
                    },
                    {
                        'id': 3,
                        'name': 'Участие в мероприятиях',
                        'rating': None,
                        'data': { 'value': 0, },
                    },
                ],
                'stages': [
                    {
                        'id': i + 1,
                        'time': None,
                        'data': { 'comment': None, },
                        'rejection': False,
                        'active': False,
                    } for i in range(6)
                ]
            }
            for item in result:
                k = str(item.id)
                user_activity = { 'time_last_activity': activity[k] if k in activity else None }
                events_pendings = {}
                if len(result) == 1:
                     events_pendings = { 'events_confirmations_pendings': await item.get_events_confirmations_pendings() }
                users.append(item.dump() | user_activity | { 'membership': memberships[k] if k in memberships else membership_template } | events_pendings)
            if request.params['filter']:
                users = [ { k: user[k] for k in request.params['filter'] } for user in users ]
            return OrjsonResponse({
                'users': users,
                'amount': amount,
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_update']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or user.community_manager_id == request.user.id:
                    temp = User()
                    if await temp.find(email = request.params['email']):
                        if temp.id != user.id:
                            return err(400, 'Email уже зарегистрирован')
                    temp = User()
                    if await temp.find(phone = request.params['phone']):
                        if temp.id != user.id:
                            return err(400, 'Телефон уже зарегистрирован')
                    args = request.params | { 'roles': user.roles }
                    await user.update(**args)
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_membership_stage_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params | request.path_params, MODELS['manager_user_membership_stage_update']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or user.community_manager_id == request.user.id:
                    await user.membership_stage_update(
                        stage_id = request.params['stage_id'],
                        field = request.path_params['field'], 
                        value = request.params['value'],
                        author_id = request.user.id,
                    )
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_membership_rating_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params | request.path_params, MODELS['manager_user_membership_rating_update']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or user.community_manager_id == request.user.id:
                    await user.membership_rating_update(
                        field = request.path_params['field'], 
                        value = request.params['value'],
                        author_id = request.user.id,
                    )
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            community_manager_id = None
            if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
                community_manager_id = request.params['community_manager_id']
            else:
                community_manager_id = request.user.id
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = request.params['company'],
                position = request.params['position'],
                catalog = request.params['catalog'],
                password = request.params['password'],
                roles = [ 'client' ],
                active = request.params['active'],
                detail = request.params['detail'],
                status = request.params['status'],
                city = request.params['city'],
                hobby = request.params['hobby'],
                tags = request.params['tags'],
                interests = request.params['interests'],
                annual = request.params['annual'],
                annual_privacy = request.params['annual_privacy'],
                employees = request.params['employees'],
                employees_privacy = request.params['employees_privacy'],
                birthdate = request.params['birthdate'],
                birthdate_privacy = request.params['birthdate_privacy'],
                experience = request.params['experience'],
                community_manager_id = community_manager_id,
            )
            dispatch('user_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_confirm_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_confirm_event']):
            user = User()
            await user.set(id = request.params['user_id'])
            if user.id:
                event = Event()
                await event.set(id = request.params['event_id'])
                if event.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or \
                            user.community_manager_id == request.user.id:
                        await user.confirm_event(event_id = event.id)
                        dispatch('user_update', request)
                        return OrjsonResponse({
                            'event_id': event.id,
                            'user_id': user.id,
                            'confirmation': True,
                        })
                    else:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Событие не найдено')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_add_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_add_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or \
                            user.community_manager_id == request.user.id:
                        await user.add_event(event_id = event.id)
                        await user.confirm_event(event_id = event.id)
                        dispatch('user_add_event', request)
                        return OrjsonResponse({})
                    else:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Пользователь не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
        


################################################################
async def manager_user_del_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_del_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or \
                            user.community_manager_id == request.user.id:
                        await user.del_event(event_id = event.id)
                        dispatch('user_update', request)
                        return OrjsonResponse({})
                    else:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Пользователь не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_audit_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_audit_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or \
                            user.community_manager_id == request.user.id:
                        await user.audit_event(event_id = event.id, audit = request.params['audit'])
                        dispatch('user_update', request)
                        return OrjsonResponse({})
                    else:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Пользователь не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')

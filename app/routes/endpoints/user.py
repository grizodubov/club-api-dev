from datetime import datetime
import asyncio
import re
from random import randint, choices, sample
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User, get_residents, get_speakers, get_residents_contacts, get_community_managers, get_telegram_pin, get_last_activity, get_users_memberships, get_agents_list, get_agents, create_connection, recover_connection, drop_connection, update_connection_state, update_connection_comment, get_connections, update_connection_rating, get_profiles_views_amount, get_date_profiles_views_amount
from app.models.event import Event, get_events_confirmations_pendings
from app.models.item import Item
from app.models.note import get_last_notes_times
from app.helpers.mobile import send_mobile_message
from app.models.notification import create_notifications
from app.models.notification_1 import create_multiple as create_notification_1_multi



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
        Route('/user/profile/view', user_view_profile, methods = [ 'POST' ]),

        Route('/user/{id:int}/helpful', user_helpful, methods = [ 'POST' ]),

        Route('/user/telegram/get/pin', save_telegram_pin, methods = [ 'POST' ]),

        Route('/m/user/search', moderator_user_search, methods = [ 'POST' ]),
        Route('/m/user/for/select', moderator_user_for_select, methods = [ 'POST' ]),
        Route('/m/user/update', moderator_user_update, methods = [ 'POST' ]),
        Route('/m/user/create', moderator_user_create, methods = [ 'POST' ]),

        Route('/new/user/residents', user_residents, methods = [ 'POST' ]),
        Route('/new/user/resident/{id:int}', new_user_resident, methods = [ 'POST' ]),
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
        Route('/ma/user/event/guests', manager_user_guests_event, methods = [ 'POST' ]),

        Route('/ma/user/control/update', manager_user_control_update, methods = [ 'POST' ]),

        Route('/ma/user/agent/search', manager_agent_search, methods = [ 'POST' ]),
        Route('/ma/user/agent/create', manager_agent_create, methods = [ 'POST' ]),

        Route('/ma/user/agent/list', manager_agent_list, methods = [ 'POST' ]),

        Route('/ma/user/connection/add', manager_user_connection_add, methods = [ 'POST' ]),
        Route('/ma/user/connection/del', manager_user_connection_del, methods = [ 'POST' ]),
        Route('/ma/user/connection/rec', manager_user_connection_rec, methods = [ 'POST' ]),
        Route('/ma/user/connection/state', manager_user_connection_state, methods = [ 'POST' ]),
        Route('/ma/user/connection/comment', manager_user_connection_comment, methods = [ 'POST' ]),
        Route('/ma/user/connection/rating', manager_user_connection_rating, methods = [ 'POST' ]),
        Route('/ma/user/profiles/views', manager_user_profile_views, methods = [ 'POST' ]),
        
        Route('/ma/user/suggestions', manager_user_suggestions, methods = [ 'POST' ]),
        Route('/ma/user/events/summary', manager_user_events_summary, methods = [ 'POST' ]),
        Route('/ma/user/views/summary', manager_user_views_summary, methods = [ 'POST' ]),
        Route('/ma/user/contacts', manager_user_contacts, methods = [ 'POST' ]),
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
    'user_view_profile': {
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_profile_views': {
        'date': {
			'required': True,
			'type': 'str',
        },
        'users_ids': {
			'required': True,
			'type': 'int',
            'list': True,
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
        'inn': {
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
        'inn': {
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
            'values': [ 'admin', 'client', 'guest', 'manager', 'moderator', 'editor', 'chief', 'community manager', 'tester', 'speaker', 'agent', 'curator', 'organizer' ],
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
    #
    'new_user_resident': {
        'id': {
            'required': False,
			'type': 'int',
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
        'id': {
            'required': True,
            'type': 'int',
        },
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
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if x and x.strip() and re.fullmatch(r'\d\d\/\d\d\/\d\d\d\d', re.sub(r'\s', '', x.strip())) else None,
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
        'tags_1_company_scope': {
			'required': True,
			'type': 'str',
		},
        'tags_1_company_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_expertise': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_licenses': {
			'required': True,
			'type': 'str',
		},
        'tags_1_hobbies': {
			'required': True,
			'type': 'str',
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
        'inn': {
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
            'values': [ 'admin', 'client', 'guest', 'manager', 'moderator', 'editor', 'chief', 'community manager', 'tester', 'speaker', 'agent', 'curator', 'organizer' ],
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
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if x and x.strip() and re.fullmatch(r'\d\d\/\d\d\/\d\d\d\d', re.sub(r'\s', '', x.strip())) else None,
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
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'agent_id': {
			'type': 'int',
            'null': True,
		},
        'curator_id': {
			'type': 'int',
            'null': True,
		},
        'tags_1_company_scope': {
			'required': True,
			'type': 'str',
		},
        'tags_1_company_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_expertise': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_licenses': {
			'required': True,
			'type': 'str',
		},
        'tags_1_hobbies': {
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
        'inn': {
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
            'values': [ 'client', 'guest', 'manager', 'chief', 'community manager', 'agent', 'curator' ],
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
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if x and x.strip() and re.fullmatch(r'\d\d\/\d\d\/\d\d\d\d', re.sub(r'\s', '', x.strip())) else None,
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
		'link_telegram': {
			'required': True,
			'type': 'str',
		},
		'community_manager_id': {
			'type': 'int',
            'null': True,
		},
		'agent_id': {
			'type': 'int',
            'null': True,
		},
        'curator_id': {
			'type': 'int',
            'null': True,
		},
        'tags_1_company_scope': {
			'required': True,
			'type': 'str',
		},
        'tags_1_company_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_expertise': {
			'required': True,
			'type': 'str',
		},
        'tags_1_personal_needs': {
			'required': True,
			'type': 'str',
		},
        'tags_1_licenses': {
			'required': True,
			'type': 'str',
		},
        'tags_1_hobbies': {
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
        'show_events_confirmations_pendings': {
            'required': True,
            'type': 'bool',
            'default': False,
        },
        'show_notes_times': {
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
		'inn': {
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
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if x and x.strip() and re.fullmatch(r'\d\d\/\d\d\/\d\d\d\d', re.sub(r'\s', '', x.strip())) else None,
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
		'agent_id': {
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
            'value_min': 0,
            'value_max': 6,
        },
        'field': {
            'required': True,
			'type': 'str',
            'values': [ 'comment', 'time_control', 'rejection', 'postopen', 'repeat', 'active' ],
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
		'inn': {
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
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y") if x and x.strip() and re.fullmatch(r'\d\d\/\d\d\/\d\d\d\d', re.sub(r'\s', '', x.strip())) else None,
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
        'agent_id': {
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
			'type': 'int',
            'values': [ 0, 1, 2 ]
		},
    },
    'manager_user_guests_event': {
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
        'guests': {
			'required': True,
			'type': 'int',
            'values': [ 0, 1, 2, 3, 4, 5 ]
		},
    },
    'manager_user_control_update': {
		'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'field': {
            'required': True,
			'type': 'str',
            'values': [ 'time_control' ],
        },
        'value': {
            'required': True,
			'type': 'str',
            'null': True,
        },
    },
	'manager_agent_search': {
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
	'manager_agent_create': {
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
	},
    'manager_user_connection_add': {
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_1_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_2_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_connection_del': {
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_1_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_2_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_connection_rec': {
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_1_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_2_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_connection_state': {
        'connection_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'state': {
            'required': True,
			'type': 'bool',
        }
    },
    'manager_user_connection_comment': {
        'connection_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'comment': {
            'required': True,
			'type': 'str',
            'length_min': 1,
        }
    },
    'manager_user_connection_rating': {
        'connection_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'part': {
            'required': True,
			'type': 'int',
            'values': [ 1, 2 ],
        },
        'rating': {
            'required': True,
			'type': 'int',
            'values': [ 0, 1, 2 ],
        }
    },
    'manager_user_suggestions': {
        'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_events_summary': {
        'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
    },
    'manager_user_views_summary': {
        'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'date': {
			'required': True,
			'type': 'str',
        },
    },
    'manager_user_contacts': {
        'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
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
async def user_view_profile(request):
    if request.user.id:
        if validate(request.params, MODELS['user_view_profile']):
            result = await request.user.view_profile(user_id = request.params['user_id'])
            if result:
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
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
        data = await request.user.get_suggestions_new()
        ### remove data for roles
        roles = set(request.user.roles)
        roles.discard('applicant')
        roles.discard('guest')
        result = {
            'tags_all': [],
            'interests_all': [],
            'tags': [],
            'interests': [],
        }
        for item in data:
            if not roles:
                item['company'] = ''
                item['position'] = ''
                item['link_telegram'] = ''
            if item['offer'] == 'bid':
                result['tags_all'].append(item)
            if item['offer'] == 'ask':
                result['interests_all'].append(item)
        temp = []
        if result['tags_all']:
            #result['tags'] = sample(result['tags_all'], k = min(3, len(result['tags_all'])))
            result['tags'] = result['tags_all']
            temp = [ item['id'] for item in result['tags'] ]
        if result['interests_all']:
            #result['interests'] = sample(list(filter(lambda x: x['id'] not in temp, result['interests_all'])), k = min(3, len(result['interests_all'])))
            result['interests'] = [ item for item in result['interests_all'] if item['id'] not in temp ]
        return OrjsonResponse(result | { 'self_tags': request.user.tags_1_company_scope, 'self_interests': request.user.tags_1_company_needs })
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
            agents = await get_agents()
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
                'agents': agents,
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
            # result2 = await get_speakers(None)
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
            # speakers = []
            # for item in result2:
            #     temp = item.show()
            #     if not roles and request.user.id != temp['id']:
            #         temp['company'] = ''
            #         temp['position'] = ''
            #         temp['link_telegram'] = ''
            #     speakers.append(temp)
            return OrjsonResponse({
                'residents': residents,
                'contacts': contacts,
                # 'speakers': speakers,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_user_resident(request):
    if request.user.id:
        if validate(request.path_params, MODELS['new_user_resident']):
            result = await get_residents(users_ids = [ request.path_params['id'], request.user.id ])
            result.extend(await get_speakers(users_ids = [ request.path_params['id'], request.user.id ]))
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
            if request.params['id'] == request.user.id:
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
                user = User()
                await user.set(id = request.params['id'])
                if user.id:
                    if (request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' })) or \
                            (request.user.check_roles({ 'community manager' }) and request.user.id == user.community_manager_id):
                        await user.update(**request.params)
                        dispatch('user_update', request)
                        await user.set(id = request.params['id'])
                        result = user.dshow()
                        result.update({ 
                            'contact': False,
                            'allow_contact': False
                        })
                        return OrjsonResponse(result)
                else:
                    return err(404, 'Пользователь не найден')
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
                inn = request.params['inn'],
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
                agent_id = request.params['agent_id'],
                curator_id = request.params['curator_id'],
                tags_1_company_scope = request.params['tags_1_company_scope'],
                tags_1_company_needs = request.params['tags_1_company_needs'],
                tags_1_personal_expertise = request.params['tags_1_personal_expertise'],
                tags_1_personal_needs = request.params['tags_1_personal_needs'],
                tags_1_licenses = request.params['tags_1_licenses'],
                tags_1_hobbies = request.params['tags_1_hobbies'],
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
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager', 'agent', 'curator' }):
        if validate(request.params, MODELS['manager_user_search']):
            community_manager_id = None
            agent_id = None
            access = request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' })
            if not access:
                if not request.params['ignore_community_manager']:
                    community_manager_id = request.user.id
                if not request.user.check_roles({ 'community manager' }):
                    agent_id = await request.user.get_agent_subs_tree()
                    if request.user.check_roles({ 'agent' }):
                        agent_id.append(request.user.id)
            (result, amount) = await User.client_search(
                text = request.params['text'],
                ids = request.params['ids'],
                community_manager_id = community_manager_id,
                agent_id = agent_id,
                active_only = False,
                offset = (request.params['page'] - 1) * 15 if request.params['page'] else None,
                limit = 15 if request.params['page'] else None,
                count = True,
                inn = True,
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
            events_pendings = {}
            if request.params['show_events_confirmations_pendings']:
                events_pendings = await get_events_confirmations_pendings()
            notes_times = {}
            if request.params['show_notes_times']:
                notes_times = await get_last_notes_times(users_ids = users_ids)
            for item in result:
                k = str(item.id)
                user_activity = { 'time_last_activity': activity[k] if k in activity else None }
                temp = {}
                if k in events_pendings:
                    temp.update({ 'events_confirmations_pendings': events_pendings[k] })
                if k in notes_times:
                    temp.update({ 'notes_last_time': notes_times[k] })
                hide_password = { '_password': '' }
                if access or item.community_manager_id == request.user.id:
                    hide_password = {}
                users.append(
                    item.dump() |
                    user_activity |
                    { 'membership': memberships[k] if k in memberships else membership_template } |
                    temp |
                    hide_password
                )
            if request.params['filter']:
                users = [ { k: user[k] if k in user else None for k in request.params['filter'] } for user in users ]
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
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager', 'agent' }):
        if validate(request.params | request.path_params, MODELS['manager_user_membership_stage_update']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                current_stage_id = await user.get_membership_stage()
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or \
                        user.community_manager_id == request.user.id or \
                        (user.agent_id == request.user.id and current_stage_id == 0 and request.params['stage_id'] == 0 and request.path_params['field'] == 'repeat'):
                    # notify
                    if request.params['stage_id'] == 0 and request.path_params['field'] == 'active' and request.params['value'] == 'true':
                        if current_stage_id != 0:
                            create_notifications('return_to_agent', request.user.id, user.id, {})
                    if (request.path_params['field'] != 'time_control' or (request.params['stage_id'] != 4 and request.params['stage_id'] != 5) or request.user.check_roles({ 'admin', 'chief' })):
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
                return err(400, 'Email уже зарегистрирован. Пользователь \'' + user.name + '\'')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован. Пользователь \'' + user.name + '\'')
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
                inn = request.params['inn'],
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
                curator_id = None,
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
                    if request.user.check_roles({ 'admin', 'moderator', 'chief', 'organizer' }) or \
                            user.community_manager_id == request.user.id:
                        await user.audit_event(event_id = event.id, audit = request.params['audit'])
                        if request.params['audit'] == 2:
                            create_notifications('user_arrive', request.user.id, user.id, request.params)
                            connections_ids = await user.get_event_connections_ids(event_id = event.id)
                            if connections_ids:
                                await create_notification_1_multi(
                                    users_ids = connections_ids,
                                    event = 'arrive', 
                                    data = {
                                        'user': {
                                            'id': user.id,
                                            'name': user.name,
                                        },
                                        'event': {
                                            'id': event.id,
                                            'time_event': event.time_event,
                                            'format': event.format,
                                            'name': event.name,
                                        }
                                    }
                                )
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
async def manager_user_guests_event(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_guests_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'chief', 'organizer' }) or \
                            user.community_manager_id == request.user.id:
                        await user.guests_event(event_id = event.id, guests = request.params['guests'])
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
async def manager_user_control_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_control_update']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or user.community_manager_id == request.user.id:
                    await user.control_update(
                        field = request.params['field'], 
                        value = request.params['value'],
                        #author_id = request.user.id,
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
async def manager_agent_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_agent_search']):
            community_manager_id = None
            access = request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' })
            (result, amount) = await User.agent_search(
                text = request.params['text'],
                ids = request.params['ids'],
                community_manager_id = community_manager_id,
                active_only = False,
                offset = (request.params['page'] - 1) * 15 if request.params['page'] else None,
                limit = 15 if request.params['page'] else None,
                count = True,
            )
            users_ids = [ user.id for user in result ]
            activity = await get_last_activity(users_ids = users_ids)
            users = []
            for item in result:
                k = str(item.id)
                user_activity = { 'time_last_activity': activity[k] if k in activity else None }
                hide_password = { '_password': '' }
                if access or item.community_manager_id == request.user.id:
                    hide_password = {}
                users.append(item.dump() | user_activity | hide_password)
            if request.params['filter']:
                users = [ { k: user[k] if k in user else None for k in request.params['filter'] } for user in users ]
            return OrjsonResponse({
                'users': users,
                'amount': amount,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_agent_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_agent_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            password = ''.join(choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_!@#$%&*', k = 9))
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = '',
                position = '',
                catalog = '',
                password = password,
                roles = [ 'agent' ],
                active = True,
                community_manager_id = None,
                curator_id = None,
            )
            dispatch('user_create', request)
            return OrjsonResponse({
                'id': user.id,
                'name': user.name,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_agent_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        community_manager_id = None
        if not request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
            community_manager_id = request.user.id
        agents = await get_agents_list(community_manager_id = community_manager_id)
        community_managers = await get_community_managers()
        return OrjsonResponse({
            'agents': agents,
            'amount': len(agents),
            'community_managers': community_managers,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_connection_add(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_add']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user1 = User()
                await user1.set(id = request.params['user_1_id'])
                user2 = User()
                await user2.set(id = request.params['user_2_id'])
                if user1.id and user2.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'chief' }) or \
                            user1.community_manager_id == request.user.id or \
                            user2.community_manager_id == request.user.id:
                        await create_connection(event_id = event.id, user_1_id = user1.id, user_2_id = user2.id, creator_id = request.user.id)
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
async def manager_user_connection_del(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_del']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user1 = User()
                await user1.set(id = request.params['user_1_id'])
                user2 = User()
                await user2.set(id = request.params['user_2_id'])
                if user1.id and user2.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'chief' }) or \
                            user1.community_manager_id == request.user.id or \
                            user2.community_manager_id == request.user.id:
                        await drop_connection(event_id = event.id, user_1_id = user1.id, user_2_id = user2.id)
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
async def manager_user_connection_rec(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_rec']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user1 = User()
                await user1.set(id = request.params['user_1_id'])
                user2 = User()
                await user2.set(id = request.params['user_2_id'])
                if user1.id and user2.id:
                    if request.user.check_roles({ 'admin', 'moderator', 'chief' }) or \
                            user1.community_manager_id == request.user.id or \
                            user2.community_manager_id == request.user.id:
                        await recover_connection(event_id = event.id, user_1_id = user1.id, user_2_id = user2.id)
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
async def manager_user_connection_state(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_state']):
            connections = await get_connections(ids = [ request.params['connection_id'] ])
            if connections:
                connection = connections[0]
                user1 = User()
                await user1.set(id = connection['user_1_id'])
                user2 = User()
                await user2.set(id = connection['user_2_id'])
                if request.user.check_roles({ 'admin', 'moderator', 'chief' }) or \
                        user1.community_manager_id == request.user.id or \
                        user2.community_manager_id == request.user.id:
                    await update_connection_state(connection['id'], request.params['state'])
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Стыковка не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_connection_comment(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_comment']):
            connections = await get_connections(ids = [ request.params['connection_id'] ])
            if connections:
                connection = connections[0]
                user1 = User()
                await user1.set(id = connection['user_1_id'])
                user2 = User()
                await user2.set(id = connection['user_2_id'])
                if request.user.check_roles({ 'admin', 'moderator', 'chief' }) or \
                        user1.community_manager_id == request.user.id or \
                        user2.community_manager_id == request.user.id:
                    await update_connection_comment(connection['id'], request.params['comment'], request.user.id)
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Стыковка не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_connection_rating(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_connection_rating']):
            connections = await get_connections(ids = [ request.params['connection_id'] ])
            if connections:
                connection = connections[0]
                user1 = User()
                await user1.set(id = connection['user_1_id'])
                user2 = User()
                await user2.set(id = connection['user_2_id'])
                part = 0
                if request.user.check_roles({ 'admin', 'moderator', 'chief' }):
                    part = request.params['part']
                if user1.community_manager_id == request.user.id:
                    part = 1
                if user2.community_manager_id == request.user.id:
                    part = 2
                if user1.community_manager_id == user2.community_manager_id and part:
                    part = 3
                if part:
                    await update_connection_rating(connection['id'], part, request.params['rating'])
                    dispatch('user_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Стыковка не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_profile_views(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_profile_views']):
            amount = await get_profiles_views_amount(request.params['users_ids'])
            amount_date = await get_date_profiles_views_amount(request.params['users_ids'], request.params['date'])
            result = {}
            for k, v in amount.items():
                result[k] = {
                    'amount': v,
                    'amount_date': 0,
                }
            for k, v in amount_date.items():
                if k in result:
                    result[k]['amount_date'] = v
            return OrjsonResponse({
                'views': result,
                'date': request.params['date'],
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_suggestions(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_suggestions']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                data = await user.get_suggestions_new()
                return OrjsonResponse({
                    'suggestions': data,
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_events_summary(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_events_summary']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                data = await user.get_events_summary()
                return OrjsonResponse(data)
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_views_summary(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_views_summary']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                stats = await user.get_profile_views_amount(request.params['date'])
                log = await user.get_profile_views()
                return OrjsonResponse({
                    'stats': stats,
                    'log': log,
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_user_contacts(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_user_contacts']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                contacts = await user.get_contacts()
                return OrjsonResponse({
                    'contacts': contacts,
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')

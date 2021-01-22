from typing import Any, Text, Dict, List
import json
from rasa_sdk.events import AllSlotsReset
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import SlotSet
from rasa_sdk.events import UserUtteranceReverted
from rasa_sdk.knowledge_base.storage import InMemoryKnowledgeBase
from rasa_sdk.knowledge_base.actions import ActionQueryKnowledgeBase
from rasa_sdk.forms import FormAction
import sqlite3
import time
import datetime
from datetime import datetime, timedelta

db_path = "C:\\Users\\vansh\\PycharmProjects\\twitterbot\\rasa2\\.spotter\\jarvis.db"
INTENT_DESCRIPTION_MAPPING_PATH = "actions/intent_mapping.csv"

def createskill(sender_id, skill_name , skill_unit, skill_tag):
    #create a new skill in db with given details
    #creating timestamp
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(db_path) as db:
        cur = db.cursor()
        sqlQuery = "select skill_name FROM jarvis_skills_list WHERE sender_id = " + "'"+sender_id+"'" ";"
        cur.execute(sqlQuery)
        result = cur.fetchall()

    result_list = []
    for i in result:
        result_list.append(i[0])
    result = result_list

    if skill_name in result:
        return False
    else:

        sqlQuery = "INSERT INTO jarvis_skills_list (timestamp,sender_id,skill_name,skill_unit,skill_tag) VALUES (" + "'"+ st+"'" + "," +"'"+ sender_id +"'"+ "," + "'"+skill_name+"'" + "," + "'"+skill_unit+"'" + "," + "'"+skill_tag +"'"+");"
        print(sqlQuery)
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            db.commit()

        with open("C:\\Users\\vansh\\PycharmProjects\\twitterbot\\rasa2\\.spotter\\data\\test\\lookup_tables\\skills.txt", "a") as f:
            txt ="\n"+skill_name
            f.write(txt)

        return True

def addskill(sender_id,skill_name,skill_qty):

# add skill repetition eg. add 15 pushups
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(db_path) as db:
        cur = db.cursor()
        sqlQuery = "select skill_name FROM jarvis_skills_list WHERE sender_id = " + "'"+sender_id+"'" ";"
        cur.execute(sqlQuery)
        result = cur.fetchall()

    result_list = []
    for i in result:
        result_list.append(i[0])
    result = result_list

    if skill_name in result:
        sqlQuery = "INSERT INTO jarvis_skill (timestamp,sender_id,skill_name,skill_qty) VALUES (" + "'" + st + "'" + "," + "'" + sender_id + "'" + "," + "'" + skill_name + "'" + "," + "'" + skill_qty + "');"
        print(sqlQuery)
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            db.commit()

        sqlQuery = "SELECT SUM(skill_qty) FROM  jarvis_skill WHERE timestamp >" + datetime.fromtimestamp(
            ts).strftime('%Y-%m-%d') + " AND sender_id = '" + sender_id + "'" + " AND skill_name = '"+ skill_name+"';"
        print(sqlQuery)
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            result = cur.fetchall()
        return result[0][0]
    else:
        return False

def removeskill(sender_id,skill_name):
#     TODO
# remove all data for given skill name
    # check if the skill is present or not
    with sqlite3.connect(db_path) as db:
        cur = db.cursor()
        sqlQuery = "select skill_name FROM jarvis_skills_list WHERE sender_id = " + "'"+sender_id+"'" ";"
        cur.execute(sqlQuery)
        result = cur.fetchall()


    result_list = []
    for i in result:
        result_list.append(i[0])
    result = result_list

    if skill_name in result:
        sqlQuery = "DELETE FROM jarvis_skills_list WHERE skill_name ='"+skill_name + "' AND sender_id = '" + sender_id + "' ;"
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            db.commit()
        sqlQuery = "DELETE FROM jarvis_skill WHERE skill_name ='"+skill_name + "' AND sender_id = '" + sender_id + "' ;"
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            db.commit()
        return True
    else:
        return False

def getskill(sender_id):
    # TODO
# get list of all unique skills for sender
    with sqlite3.connect(db_path) as db:
        cur = db.cursor()
        sqlQuery = "select skill_name FROM jarvis_skills_list WHERE sender_id = " + "'"+sender_id+"'" ";"
        cur.execute(sqlQuery)
        result = cur.fetchall()
    result_list = []
    for i in result:
        result_list.append(i[0])
    result = result_list

    return result

def reporting(report_name,skill_details,dates):
    # TO DO
    # inputs : report_name is name of the report_name (text)
    # skill_details: tuple carrying (sender_id, skill_name, skill_tag)
    # dates: (start_date,end_date) tuple carrying details
    # return text and image of the report
    # pull the required skill data based on skill_details and dates into a pandas dataframe and create a report text
    # and report chart

    sender_id = skill_details[0]
    skill_name = skill_details[1]
    skill_tag = skill_details[2]
    start_date = dates[0]
    end_date = dates[1]

    print(report_name, skill_details, dates)


    if report_name == 'skill_sum':
        sqlQuery = "SELECT SUM(skill_qty) FROM jarvis_skill WHERE sender_id = '"+sender_id+"' AND skill_name ='"+skill_name+"' AND timestamp >= '"+start_date+"' AND timestamp < '"+end_date+"';"
        print(sqlQuery)
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            result = cur.fetchone()
        return (str(result[0]),None)

    if report_name == 'all_skill_sum':
        sqlQuery = "select skill_name,SUM(skill_qty) from jarvis_skill WHERE (sender_id = '"+sender_id+"' AND timestamp >= '"+start_date+"' AND timestamp < '"+end_date+"') Group by skill_name;"
        print(sqlQuery)
        with sqlite3.connect(db_path) as db:
            cur = db.cursor()
            cur.execute(sqlQuery)
            result = cur.fetchall()

        print(result)
        string = 'Here is what I have'
        for i in result:
            name = i[0]
            value = i[1]
            string = string+ '\n'+name+' '+str(value)

        if result == []:
            string = string + '\n' + ' Nothing ! '

        return (string,None)


class ActionCheckSkill(Action):

    def name(self) -> Text:
        return "action_check_skill"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:


        skill_name = tracker.get_slot('skill_name')
        skill_unit = tracker.get_slot('skill_unit')
        skill_tag = tracker.get_slot('skill_tag')
        sender_id = tracker.current_state()['sender_id']

        if not createskill(sender_id, skill_name.lower() , skill_unit.lower(), skill_tag.lower()):
            txt = "You already have this skill. Just start updating it, for example say ADD 10 " + skill_name
            dispatcher.utter_message(text=txt)
            return [AllSlotsReset()]
        else:
            txt= "Skill type "+skill_name+" created successfully. \nJust start updating it, for example say ADD 10 " + skill_name

        # utter submit template
            dispatcher.utter_message(text=txt)

            return [AllSlotsReset()]

class ActionSlotReset(Action):

    def name(self) -> Text:
        return "action_slot_reset"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        return [AllSlotsReset()]

class ActionAddskill(Action):

    def name(self) -> Text:
        return "action_add_skill"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        skill_qty = tracker.get_slot('skill_qty')
        skill_name = tracker.get_slot('skill_name')
        sender_id = tracker.current_state()['sender_id']

        today_qty = addskill(sender_id,skill_name.lower(),skill_qty)

        if not today_qty:
            dispatcher.utter_message(text="This skill is not available, please create this skill first.")
        else:
            txt = "Added . \n\n Total " + str(today_qty) +" "+ skill_name + " for today."
        # dispatch button with text
            dispatcher.utter_message(text=txt)

        return [AllSlotsReset()]

class ActionRemoveskill(Action):

    def name(self) -> Text:
        return "action_remove_skill"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        skill_name = tracker.get_slot('skill_name')
        skill_remove_conf = tracker.get_slot('skill_remove_conf')
        sender_id = tracker.current_state()['sender_id']
        print(skill_remove_conf)

        if skill_remove_conf == 'Yes':

            if removeskill(sender_id,skill_name.lower()):
                txt = "Done! \n" + skill_name +" skill removed. Along with all the data."
            # dispatch button with text
                dispatcher.utter_message(text=txt)
            else:
                dispatcher.utter_message(text="I cannot find this skill anyway")
        else:
            dispatcher.utter_message(template="utter_rejecting_skill_removal")

        return [AllSlotsReset()]

class Actionshowallskill(Action):

    def name(self) -> Text:
        return "action_show_all_skills"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        skill_name = tracker.get_slot('skill_name')
        sender_id = tracker.current_state()['sender_id']
        # skill_list = getskill(sender_id) -> List
        all_skills =  "\n".join(getskill(sender_id))
        txt = "You got the following skills: \n" + all_skills

        # dispatch button with text
        dispatcher.utter_message(text=txt)

        return []

class ActionDefaultAskAffirmation(Action):
    """Asks for an affirmation of the intent if NLU threshold is not met."""

    def name(self) -> Text:
        return "action_default_ask_affirmation"

    def __init__(self) -> None:
        import pandas as pd

        self.intent_mappings = pd.read_csv(INTENT_DESCRIPTION_MAPPING_PATH)
        self.intent_mappings.fillna("", inplace=True)
        self.intent_mappings.entities = self.intent_mappings.entities.map(
            lambda entities: {e.strip() for e in entities.split(",")}
        )

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]
    ) :

        intent_ranking = tracker.latest_message.get("intent_ranking", [])
        if len(intent_ranking) > 1:
            diff_intent_confidence = intent_ranking[0].get(
                "confidence"
            ) - intent_ranking[1].get("confidence")
            if diff_intent_confidence < 0.2:
                intent_ranking = intent_ranking[:2]
            else:
                intent_ranking = intent_ranking[:1]

        # for the intent name used to retrieve the button title, we either use
        # the name of the name of the "main" intent, or if it's an intent that triggers
        # the response selector, we use the full retrieval intent name so that we
        # can distinguish between the different sub intents
        first_intent_names = [
            intent.get("name", "")
            if intent.get("name", "") not in ["out_of_scope", "faq", "chitchat"]
            else tracker.latest_message.get("response_selector")
            .get(intent.get("name", ""))
            .get("full_retrieval_intent")
            for intent in intent_ranking
        ]

        message_title = (
            "Sorry, I'm not sure I've understood " "you correctly 🤔 Do you mean..."
        )

        entities = tracker.latest_message.get("entities", [])
        entities = {e["entity"]: e["value"] for e in entities}

        entities_json = json.dumps(entities)

        buttons = []
        for intent in first_intent_names:
            button_title = self.get_button_title(intent, entities)
            if "/" in intent:
                # here we use the button title as the payload as well, because you
                # can't force a response selector sub intent, so we need NLU to parse
                # that correctly
                buttons.append({"title": button_title, "payload": button_title})
            else:
                buttons.append(
                    {"title": button_title, "payload": f"/{intent}{entities_json}"}
                )

        buttons.append({"title": "Something else", "payload": "/out_of_scope"})

        dispatcher.utter_message(text=message_title, buttons=buttons)

        return []

    def get_button_title(self, intent: Text, entities: Dict[Text, Text]) -> Text:
        default_utterance_query = self.intent_mappings.intent == intent
        utterance_query = (self.intent_mappings.entities == entities.keys()) & (
            default_utterance_query
        )

        utterances = self.intent_mappings[utterance_query].button.tolist()

        if len(utterances) > 0:
            button_title = utterances[0]
        else:
            utterances = self.intent_mappings[default_utterance_query].button.tolist()
            button_title = utterances[0] if len(utterances) > 0 else intent

        return button_title.format(**entities)

class ActionSetGrainSlot(Action):

    def name(self) -> Text:
        return "action_set_grain"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        date = tracker.latest_message['entities'][0]['value'][0:10]
        grain = tracker.latest_message['entities'][0]['additional_info']['grain']
        intent_name = tracker.latest_message['intent']['name']
        print('intent_name  ' , type(intent_name), intent_name)
        print(date,'---',grain)

        # utter submit template
        dispatcher.utter_message(template= 'utter_checking_performance')

        skill_name = tracker.get_slot('skill_name')
        sender_id = tracker.current_state()['sender_id']
        skill_tag = tracker.get_slot('skill_tag')
        start_date = date

        date_dict = {
            "month":30,
            "week":7,
            "year":365,
            "day":1
        }

        if intent_name == 'show_performance':
            skill_name = None


        if skill_name == None:
            report_name = 'all_skill_sum'
        else:
            report_name = 'skill_sum'


        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        delta = timedelta(days=date_dict[grain])
        end_date = start_date + delta

        print(str(start_date)[0:10],' -- ', str(end_date)[0:10])

        txt, img = reporting(report_name, (sender_id,skill_name,skill_tag), (str(start_date)[0:10],str(end_date)[0:10]))
        print(txt,img)
        msg = {"type": "video", "payload": {"title": "Link name", "src": "https://youtube.com/9C1Km6xfdMA"}}
        dispatcher.utter_message(text=txt,attachment=msg)

        return []



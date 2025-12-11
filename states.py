from aiogram.fsm.state import StatesGroup, State

class Flow(StatesGroup):
    chatting = State()
    waiting_contact = State()

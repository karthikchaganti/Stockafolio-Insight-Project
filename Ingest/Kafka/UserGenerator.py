import random
import pickle
import uuid
from faker import Faker


class UserGen():

    def __init__(self,userCount):
    self.userCount = userCount
    fake = Faker()
    self.userList_dict = {}

    def userList(self):
        for i in range(int(self.userCount)):
            user_ID = str(uuid.uuid4()) # random
            user_Name =self.faker.name()
            self.userList_dict[user_ID] = user_Name
            pickle.dump(userList_dict, output)
            output.close()

if __name__ == "main":
    args = sys.argv
    userCount = str(args[1])
    obj_UserGen = UserGen(userCount)
    obj_UserGen.userList()

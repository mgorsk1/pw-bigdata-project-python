from message_proto import MSG
from config.schemas import meetup_rawdata_pb2
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson
from json import loads
from time import time
from math import ceil, floor

tries = 10000

header_length = 100
middle = "\n" + "-" * header_length + "\n"

with open('../mockups/templates/meetup-rawdata.json') as f:
    raw_string = f.read()
    raw_dict = loads(raw_string)

# ---------------------------------------------------------------------------------------------------------------------

test_scope = "CREATE MESSAGE FROM JSON"
test_scope_chars = len(test_scope) + len("TESTING") + 3
left_dash, right_dash = "-" * ceil((header_length-test_scope_chars)/2), "-" * floor((header_length-test_scope_chars)/2)

print("{} TESTING {} {}\n".format(left_dash, test_scope, right_dash))
totalp_list = list()
totalc_list = list()

for i in range(tries):

    time_start = time()
    m = MSG()
    m.ParseFromJson(raw_string)
    totalc = time() - time_start

    time_start = time()
    pm = json_format.Parse(raw_string, meetup_rawdata_pb2.MSG(), ignore_unknown_fields=False)
    totalp = time() - time_start

    totalc_list.append(totalc)
    totalp_list.append(totalp)

totalc_sum = sum(totalc_list)
totalp_sum = sum(totalp_list)
who_was_faster = ""

if totalc_sum < totalp_sum:
    who_was_faster = "Pyrobuf"
    faster = totalc_sum
    slower = totalp_sum
else:
    who_was_faster = "Google Protobuf"
    faster = totalp_sum
    slower = totalc_sum

message = "{} was {:.2f} times faster ({:.2f} vs {:.2f})".format(who_was_faster, slower / faster, faster, slower)
message_len = len(message)

left_space, right_space = " " * ceil((header_length-message_len)/2), " " * floor((header_length-message_len)/2)

print("{}{}{}".format(left_space, message, right_space))
print(middle)

# ---------------------------------------------------------------------------------------------------------------------

test_scope = "SERIALIZING TO BINARY"
test_scope_chars = len(test_scope) + len("TESTING") + 3
left_dash, right_dash = "-" * ceil((header_length-test_scope_chars)/2), "-" * floor((header_length-test_scope_chars)/2)

print("{} TESTING {} {}\n".format(left_dash, test_scope, right_dash))
totalp_list = list()
totalc_list = list()

for i in range(tries):
    c = MSG()
    c.ParseFromJson(raw_string)

    p = json_format.Parse(raw_string, meetup_rawdata_pb2.MSG(), ignore_unknown_fields=False)

    time_start = time()
    cs = c.SerializeToString()
    totalc = time() - time_start

    time_start = time()
    ps = p.SerializeToString()
    totalp = time() - time_start

    totalc_list.append(totalc)
    totalp_list.append(totalp)

totalc_sum = sum(totalc_list)
totalp_sum = sum(totalp_list)
who_was_faster = ""

if totalc_sum < totalp_sum:
    who_was_faster = "Pyrobuf"
    faster = totalc_sum
    slower = totalp_sum
else:
    who_was_faster = "Google Protobuf"
    faster = totalp_sum
    slower = totalc_sum

message = "{} was {:.2f} times faster ({:.2f} vs {:.2f})".format(who_was_faster, slower / faster, faster, slower)
message_len = len(message)

left_space, right_space = " " * ceil((header_length-message_len)/2), " " * floor((header_length-message_len)/2)

print("{}{}{}".format(left_space, message, right_space))
print(middle)

# ---------------------------------------------------------------------------------------------------------------------

test_scope = "DESERIALIZING FROM BINARY"
test_scope_chars = len(test_scope) + len("TESTING") + 3
left_dash, right_dash = "-" * ceil((header_length-test_scope_chars)/2), "-" * floor((header_length-test_scope_chars)/2)

print("{} TESTING {} {}\n".format(left_dash, test_scope, right_dash))
totalp_list = list()
totalc_list = list()

for i in range(tries):
    c = MSG()
    cn = MSG()
    c.ParseFromJson(raw_string)

    cs = c.SerializeToString()

    p = json_format.Parse(raw_string, meetup_rawdata_pb2.MSG(), ignore_unknown_fields=False)
    pn = meetup_rawdata_pb2.MSG()

    ps = p.SerializeToString()

    time_start = time()
    pn.ParseFromString(ps)
    totalc = time() - time_start

    time_start = time()
    cn.ParseFromString(cs)
    totalp = time() - time_start

    totalc_list.append(totalc)
    totalp_list.append(totalp)

totalc_sum = sum(totalc_list)
totalp_sum = sum(totalp_list)
who_was_faster = ""

if totalc_sum < totalp_sum:
    who_was_faster = "Pyrobuf"
    faster = totalc_sum
    slower = totalp_sum
else:
    who_was_faster = "Google Protobuf"
    faster = totalp_sum
    slower = totalc_sum

message = "{} was {:.2f} times faster ({:.2f} vs {:.2f})".format(who_was_faster, slower / faster, faster, slower)
message_len = len(message)

left_space, right_space = " " * ceil((header_length-message_len)/2), " " * floor((header_length-message_len)/2)

print("{}{}{}".format(left_space, message, right_space))
print(middle)

# ---------------------------------------------------------------------------------------------------------------------

test_scope = "CONVERTING TO JSON"
test_scope_chars = len(test_scope) + len("TESTING") + 3
left_dash, right_dash = "-" * ceil((header_length-test_scope_chars)/2), "-" * floor((header_length-test_scope_chars)/2)

print("{} TESTING {} {}\n".format(left_dash, test_scope, right_dash))
totalp_list = list()
totalc_list = list()

for i in range(tries):
    c = MSG()
    c.ParseFromJson(raw_string)

    p = json_format.Parse(raw_string, meetup_rawdata_pb2.MSG(), ignore_unknown_fields=False)

    time_start = time()
    cd = c.SerializeToDict()
    totalc = time() - time_start

    time_start = time()
    pd = loads(MessageToJson(p))
    totalp = time() - time_start

    totalc_list.append(totalc)
    totalp_list.append(totalp)

totalc_sum = sum(totalc_list)
totalp_sum = sum(totalp_list)
who_was_faster = ""

if totalc_sum < totalp_sum:
    who_was_faster = "Pyrobuf"
    faster = totalc_sum
    slower = totalp_sum
else:
    who_was_faster = "Google Protobuf"
    faster = totalp_sum
    slower = totalc_sum

message = "{} was {:.2f} times faster ({:.2f} vs {:.2f})".format(who_was_faster, slower / faster, faster, slower)
message_len = len(message)

left_space, right_space = " " * ceil((header_length-message_len)/2), " " * floor((header_length-message_len)/2)

print("{}{}{}".format(left_space, message, right_space))
print(middle)
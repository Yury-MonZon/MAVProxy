#!/usr/bin/env python
'''log command handling'''

import time, os

from MAVProxy.modules.lib import mp_module

class LogModule(mp_module.MPModule):
    def __init__(self, mpstate):
        super(LogModule, self).__init__(mpstate, "log", "log transfer")
        self.add_command('log', self.cmd_log, "log file handling", ['<download|status|erase|dump|resume|cancel|list>'])
        self.reset()

    def reset(self):
        self.download_set = set()
        self.download_file = None
        self.download_lognum = None
        self.download_filename = None
        self.download_start = None
        self.download_last_timestamp = None
        self.download_ofs = 0
        self.retries = 0
        self.entries = {}
        self.download_queue = []
        self.last_status = time.time()
        self.sched_rate = 50
        self.fc_ready = False
        self.dump_active = False
        self.dump_stage = 0
        self.num_logs = -1
        self.reboot_req = False

    def mavlink_packet(self, m):
        '''handle an incoming mavlink packet'''
        if m.get_type() == 'LOG_ENTRY':
            self.handle_log_entry(m)
        elif m.get_type() == 'LOG_DATA':
            self.handle_log_data(m)
        elif m.get_type() == 'STATUSTEXT':
            status_str = str(m.text)
            if 'PreArm: Waiting for RC' in status_str:
                self.fc_ready = True

    def handle_log_entry(self, m):
        '''handling incoming log entry'''
        if m.time_utc == 0:
            tstring = ''
        else:
            tstring = time.ctime(m.time_utc)
        self.num_logs = m.num_logs
        if m.num_logs == 0:
            print("No logs")
            return
        self.entries[m.id] = m
        print("Log %u  numLogs %u lastLog %u size %u %s" % (m.id, m.num_logs, m.last_log_num, m.size, tstring))


    def handle_log_data(self, m):
        '''handling incoming log data'''
        if self.download_file is None:
            return
        # lose some data
        # import random
        # if random.uniform(0,1) < 0.05:
        #    print('dropping ', str(m))
        #    return
        if m.ofs != self.download_ofs:
            self.download_file.seek(m.ofs)
            self.download_ofs = m.ofs
        if m.count != 0:
            s = bytearray(m.data[:m.count])
            self.download_file.write(s)
            self.download_set.add(m.ofs // 90)
            self.download_ofs += m.count
        self.download_last_timestamp = time.time()
        if m.count == 0 or (m.count < 90 and len(self.download_set) == 1 + (m.ofs // 90)):
            dt = time.time() - self.download_start
            self.download_file.close()
            size = os.path.getsize(self.download_filename)
            speed = size / (1000.0 * dt)
            status = "Finished downloading %s (%u bytes %u seconds, %.1f kbyte/sec %u retries)" % (self.download_filename,
                                                                                                   size,
                                                                                                   dt, speed,
                                                                                                   self.retries)
            self.console.set_status('LogDownload',status, row=4)
            print(status)
            print(len(self.download_queue))
            self.download_file = None
            self.download_filename = None
            self.download_set = set()
            self.master.mav.log_request_end_send(self.target_system,
                                                 self.target_component)
            if len(self.download_queue):
                self.log_download_next()
        self.update_status()

    def handle_log_data_missing(self):
        '''handling missing incoming log data'''
        if len(self.download_set) == 0:
            return
        highest = max(self.download_set)
        diff = set(range(highest)).difference(self.download_set)
        if len(diff) == 0:
            self.master.mav.log_request_data_send(self.target_system,
                                                       self.target_component,
                                                       self.download_lognum, (1 + highest) * 90, 0xffffffff)
            self.retries += 1
        else:
            num_requests = 0
            while num_requests < 20:
                start = min(diff)
                diff.remove(start)
                end = start
                while end + 1 in diff:
                    end += 1
                    diff.remove(end)
                self.master.mav.log_request_data_send(self.target_system,
                                                           self.target_component,
                                                           self.download_lognum, start * 90, (end + 1 - start) * 90)
                num_requests += 1
                self.retries += 1
                if len(diff) == 0:
                    break


    def log_status(self, console=False):
        '''show download status'''
        if self.download_filename is None:
            print("No download")
            return
        dt = time.time() - self.download_start
        speed = os.path.getsize(self.download_filename) / (1000.0 * dt)
        m = self.entries.get(self.download_lognum, None)
        file_size = os.path.getsize(self.download_filename)
        if m is None:
            size = 0
            pct = 0
        elif m.size == 0:
            size = 0
            pct = 100
        else:
            size = m.size
            pct = (100.0*file_size)/size
        highest = 0
        if len(self.download_set):
            highest = max(self.download_set)
        diff = set(range(highest)).difference(self.download_set)
        status = "Downloading %s - %u/%u bytes %.1f%% %.1f kbyte/s (%u retries %u missing)" % (self.download_filename,
                                                                                            os.path.getsize(self.download_filename),
                                                                                        size,
                                                                                        pct,
                                                                                        speed,
                                                                                        self.retries,
                                                                                            len(diff))
        if console:
            self.console.set_status('LogDownload', status, row=4)
            print(status)
        else:
            print(status)

    def log_download_next(self):
        if len(self.download_queue) == 0:
            return
        latest = self.download_queue.pop()
        filename = self.default_log_filename(latest)
        if os.path.isfile(filename) and os.path.getsize(filename) == self.entries.get(latest).to_dict()["size"]:
            print("Skipping existing %s" % (filename))
            self.log_download_next()
        else:
            self.log_download(latest, filename)

    def log_download_all(self):
        if len(self.entries.keys()) == 0:
            print("Please use log list first")
            return
        self.download_queue = sorted(self.entries, key=lambda id: self.entries[id].time_utc)
        self.log_download_next()

    def log_download_range(self, first, last):
        self.download_queue = sorted(list(range(first,last+1)),reverse=True)
        print(self.download_queue)
        self.log_download_next()

    def log_download_from(self,fromnum = 0):
        if len(self.entries.keys()) == 0:
            print("Please use log list first")
            return
        self.download_queue = sorted(self.entries, key=lambda id: self.entries[id].time_utc)
        self.download_queue = self.download_queue[fromnum:len(self.download_queue)]
        self.log_download_next()

    def log_download(self, log_num, filename):
        '''download a log file'''
        print("Downloading log %u as %s" % (log_num, filename))
        self.download_lognum = log_num
        self.download_file = open(filename, "wb")
        self.master.mav.log_request_data_send(self.target_system,
                                                   self.target_component,
                                                   log_num, 0, 0xFFFFFFFF)
        self.download_filename = filename
        self.download_set = set()
        self.download_start = time.time()
        self.download_last_timestamp = time.time()
        self.download_ofs = 0
        self.retries = 0

    def default_log_filename(self, log_num):
        return "log%u.bin" % log_num

    def log_list(self):
        print("Requesting log list")
        self.download_set = set()
        self.master.mav.log_request_list_send(self.target_system,
                                                   self.target_component,
                                                   0, 0xffff)

    def cmd_dump(self):
        '''dump all the logs and erase them from FC'''
        print("Log dump started")
        self.fc_ready = False
        self.num_logs = -1
        self.dump_stage = 1

    def set_sched_rate(self, rate):
        param = 'SCHED_LOOP_RATE'
        self.sched_rate = int(self.get_mav_param(param))
        print(f"Scheduler rate {self.sched_rate}Hz")
        if self.sched_rate != rate:
            self.param_set(param, rate)
            new_sched_rate = int(self.get_mav_param(param))
            print(f"Scheduler rate set to {new_sched_rate}Hz (was {self.sched_rate}Hz)")
            self.reboot_req = True

    def handle_dump_stage(self):
        if self.dump_active:
            return
        
        self.dump_active = True
        match self.dump_stage:
            case 1: # ready?
                if self.fc_ready:
                    print(">Requesting log number")
                    self.num_logs = -1
                    self.log_list() # request log number
                    self.dump_stage = 2
            case 2: # wait for log number
                if self.num_logs > 0:
                    if self.num_logs == 0:
                        print(">No logs available")
                        self.dump_stage = 0
                    else: # request scheduler
                        self.set_sched_rate(300)
                        self.dump_stage = 3
            case 3: # reboot if needed
                if self.reboot_req:
                    print("Rebooting to apply scheduler rate")
                    self.master.reboot_autopilot() 
                    self.fc_ready = False
                self.dump_stage = 4
            case 4: # wait for fc to be ready and request log number
                if self.fc_ready:
                    print(">Requesting log number again")
                    self.num_logs = -1
                    self.log_list() # request log number
                    self.dump_stage = 5
            case 5: # wait for log number and start downloading
                if self.num_logs > 0:
                    print("Downloading the logs")
                    self.log_download_all()
                    self.dump_stage = 6
                elif self.num_logs == 0:
                    print(">No logs available")
                    self.dump_stage = 0
            case 6: # wait for download to finish
                if self.download_filename is None and len(self.download_queue) == 0:
                    print("All logs downloaded successfully")
                    self.dump_stage = 7
            case 7: # revert the scheduler to the previous rate
                # self.sched_rate = max(50, self.sched_rate)
                if self.reboot_req:
                    self.set_sched_rate(self.sched_rate)
                self.dump_stage = 8
            case 8: # save params
                print("Saving parameters")
                self.mpstate.functions.process_stdin('param save after_flight.param')
                self.dump_stage = 9
            case 9: # erase the chip
                self.master.mav.log_erase_send(self.target_system, self.target_component)
                print("Log erase initiated. Allow 30s to complete")
                self.dump_stage = 0
            # case 10: # reboot if needed - don't do it - eeprom is not yet cleared
            #     if self.reboot_req:
            #         print("Rebooting to apply scheduler rate")
            #         self.master.reboot_autopilot() 
            #         self.fc_ready = False
            #     self.dump_stage = 0
        self.dump_active = False

    def cmd_log(self, args):
        '''log commands'''
        usage = "usage: log <list|download|erase|dump|resume|status|cancel>"
        if len(args) < 1:
            print(usage)
            return

        if args[0] == "status":
            self.log_status()
            
        elif args[0] == "list":
            self.log_list()

        elif args[0] == "erase":
            self.master.mav.log_erase_send(self.target_system,
                                                self.target_component)
        elif args[0] == "dump":
            self.cmd_dump()

        elif args[0] == "resume":
            self.master.mav.log_request_end_send(self.target_system,
                                                      self.target_component)
            
        elif args[0] == "cancel":
            if self.download_file is not None:
                self.download_file.close()
            self.reset()

        elif args[0] == "download":
            if len(args) < 2:
                print("usage: log download all | log download <lognumber> <filename> | log download from <lognumber>|log download range FIRST LAST")
                return
            if args[1] == 'all':
                self.log_download_all()
                return
            if args[1] == 'from':
                if len(args) < 2:
                    args[2] == 0
                self.log_download_from(int(args[2]))
                return
            if args[1] == 'range':
                if len(args) < 2:
                    print("Usage: log download range FIRST LAST")
                    return
                self.log_download_range(int(args[2]), int(args[3]))
                return
            if args[1] == 'latest':
                if len(self.entries.keys()) == 0:
                    print("Please use log list first")
                    return
                log_num = sorted(self.entries, key=lambda id: self.entries[id].time_utc)[-1]
            else:
                log_num = int(args[1])
            if len(args) > 2:
                filename = args[2]
            else:
                filename = self.default_log_filename(log_num)
            self.log_download(log_num, filename)
        else:
            print(usage)

    def update_status(self):
        '''update log download status in console'''
        now = time.time()
        if self.download_file is not None and now - self.last_status > 0.5:
            self.last_status = now
            self.log_status(True)
            
    def idle_task(self):
        '''handle missing log data'''
        if self.download_last_timestamp is not None and time.time() - self.download_last_timestamp > 0.7:
            self.download_last_timestamp = time.time()
            self.handle_log_data_missing()
        self.update_status()
        self.handle_dump_stage()
            

def init(mpstate):
    '''initialise module'''
    return LogModule(mpstate)

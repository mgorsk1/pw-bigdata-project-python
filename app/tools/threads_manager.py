from app.tools.logger import log


class ThreadsManager:
    def __init__(self, thread_arg, project_id_arg, topic_name_arg, seconds_arg=None):
        self.threads = list()
        self.counter = None
        self.threads_seconds = None

        self.project = project_id_arg
        self.topic = topic_name_arg
        self.seconds = seconds_arg

        self.thread = thread_arg

    def run(self, threads_number):
        if self.seconds:
            log.log_info("{} - Running {} threads for {} seconds".format(self.__class__.__name__,
                                                                         threads_number,
                                                                         self.seconds))
        else:
            log.log_info("{} - Running {} threads forever".format(self.__class__.__name__,
                                                                  threads_number))

        for i in range(threads_number):
            self.threads.append(self.thread(self.project, self.topic, self.seconds))

        for thread in self.threads:
            thread.join()

        self.counter = sum([t.counter for t in self.threads])
        self.time_taken = max([t.seconds for t in self.threads])

        log.log_info("{} - Summary: Processed {} objects in {:.2f} seconds. That is {:.2f} ops!".format(
                               self.__class__.__name__, self.counter, self.time_taken, self.counter / self.time_taken))

        if self.seconds:
            try:
                avg_latency = float(sum([sum(t.latencies) for t in self.threads])) / float(sum([len(t.latencies) for t in self.threads]))
            except ZeroDivisionError:
                avg_latency = 0

            log.log_info("{} - Summary: Average latency was {:.2f} ms.".format(self.__class__.__name__, avg_latency))

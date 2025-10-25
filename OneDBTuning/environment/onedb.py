# -*- coding: utf-8 -*-

import re
import os
import time
import math
import datetime
import json
import threading

import numpy as np
import environment.configs as configs
import environment.utils as utils
import environment.knobs as knobs
import requests
import psutil
import os

TEMP_FILES = os.path.abspath(".") + "/train_result/tmp/"
BEST_NOW = os.path.abspath(".") + "/tuner/"
PROJECT_DIR = os.path.abspath(".") + "/"


class OneDBEnv(object):

    def __init__(
        self,
        wk_type="read",
        num_metric=3,
        alpha=1.0,
        beta=0.5,
        time_decay=1.0,
    ):

        self.db_info = None
        self.score = 0.0
        self.steps = 0
        self.terminate = False
        self.last_external_metrics = None
        self.default_externam_metrics = None
        self.current_construct_time = 0
        self.current_query_time = 0
        self.current_knn_time = 0
        self.alpha = alpha
        self.beta = beta
        self.time_decay = time_decay
        self.num_metric = num_metric

    def _apply_knobs(self, knob):
        """Apply Knobs to the instance"""
        pass

    def eval(self, knob):
        flag = self._apply_knobs(knob)
        if not flag:
            return {"construct_time": 0, "query_time": 0, "knn_time": 0}
        external_metrics = (
            self.current_construct_time,
            self.current_query_time,
            self.current_knn_time,
        )

        # external_metrics, _ = self._get_state(knob, method=self.method)
        return {
            "construct_time": external_metrics[0],
            "query_time": external_metrics[1],
            "knn_time": external_metrics[2],
        }

    def _get_best_now(self, filename):
        best_now_path = os.path.join(BEST_NOW, filename)
        with open(best_now_path) as f:
            lines = f.readlines()
        if lines:
            
            best_now = lines[0].strip().split(",")
            construct_time_best = float(best_now[0])
            query_time_best = float(best_now[1])
            knn_time_best = float(best_now[2])
            rate_best_now = float(best_now[3])
            return construct_time_best, query_time_best, knn_time_best, rate_best_now
        else:
            raise ValueError("No data found in best now file")

    def record_best(self, external_metrics):
        filename = "bestnow.log"
        best_flag = False
        best_now_path = os.path.join(BEST_NOW, filename)

        
        construct_time_best = external_metrics[0]
        query_time_best = external_metrics[1]
        knn_time_best = external_metrics[2]
        
        rate = (construct_time_best + query_time_best + knn_time_best) / 3

        if os.path.exists(best_now_path):
            with open(best_now_path) as f:
                lines = f.readlines()
            if lines:
                best_now = lines[0].split(",")
                rate_best_now = (
                    float(best_now[0]) + float(best_now[1]) + float(best_now[2])
                ) / 3
                if rate < rate_best_now:  
                    best_flag = True
                    with open(best_now_path, "w") as f:
                        f.write(
                            f"{construct_time_best},{query_time_best},{knn_time_best},{rate}"
                        )
        else:
            with open(best_now_path, "w") as f:
                f.write(
                    f"{construct_time_best},{query_time_best},{knn_time_best},{rate}"
                )
                best_flag = True

        return best_flag

    def step(self, knob):
        """step"""
        filename = "bestnow.log"
        restart_time = utils.time_start()
        flag = self._apply_knobs(knob)
        restart_time = utils.time_end(restart_time)
        if not flag:
            return (
                -10000000.0,
                np.array([0] * self.num_metric),
                True,
                self.score - 10000000,
                [0, 0, 0],
                restart_time,
            )

        external_metrics = (
            self.current_construct_time,
            self.current_query_time,
            self.current_knn_time,
        )
        internal_metrics = np.array(
            [
                self.current_construct_time,
                self.current_query_time,
                self.current_knn_time,
            ]
        )
        reward = self._get_reward(external_metrics)
        flag = self.record_best(external_metrics)
        if flag == True:
            print("Better performance changed!")
        else:
            print("Performance remained!")
        # get the best performance so far to calculate the reward
        best_now_performance = self._get_best_now(filename)
        self.last_external_metrics = best_now_performance

        next_state = internal_metrics
        terminate = self._terminate()
        knobs.save_knobs(
            knob=knob,
            metrics=external_metrics,
            knob_file="%ssave_knobs/knob_metric.txt" % PROJECT_DIR,
        )
        return reward, next_state, terminate, self.score, external_metrics, restart_time

    def setting(self, knob):
        self._apply_knobs(knob)

    @staticmethod
    def _calculate_reward(delta0, deltat):

        if delta0 > 0:
            _reward = ((1 + delta0) ** 2 - 1) * math.fabs(1 + deltat)
        else:
            _reward = -((1 - delta0) ** 2 - 1) * math.fabs(1 - deltat)

        if _reward > 0 and deltat < 0:
            _reward = 0
        return _reward

    def _get_reward(self, external_metrics):
        """
        Args:
            external_metrics: list, external metric info, including `construct_time`, `query_time`, `knn_time`
        Return:
            reward: float, a scalar reward
        """
        print("*****************************")
        print(external_metrics)
        print(self.default_externam_metrics)
        print(self.last_external_metrics)
        print("*****************************")

        
        # delta_0_construct_time = (
        #     float((self.default_externam_metrics[0] - external_metrics[0]))
        #     / self.default_externam_metrics[0]
        # )
        # delta_t_construct_time = (
        #     float((self.last_external_metrics[0] - external_metrics[0]))
        #     / self.last_external_metrics[0]
        # )

        delta_0_query_time = (
            float((self.default_externam_metrics[1] - external_metrics[1]))
            / self.default_externam_metrics[1]
        )
        delta_t_query_time = (
            float((self.last_external_metrics[1] - external_metrics[1]))
            / self.last_external_metrics[1]
        )

        delta_0_knn_time = (
            float((self.default_externam_metrics[2] - external_metrics[2]))
            / self.default_externam_metrics[2]
        )
        delta_t_knn_time = (
            float((self.last_external_metrics[2] - external_metrics[2]))
            / self.last_external_metrics[2]
        )

        
        # construct_time_reward = self._calculate_reward(
        #     delta_0_construct_time, delta_t_construct_time
        # )
        query_time_reward = self._calculate_reward(
            delta_0_query_time, delta_t_query_time
        )
        knn_time_reward = self._calculate_reward(delta_0_knn_time, delta_t_knn_time)

        
        # reward = construct_time_reward + query_time_reward + knn_time_reward
        reward = query_time_reward + knn_time_reward
        self.score += reward

        print("$$$$$$$$$$$$$$$$$$$$$$")
        # print(construct_time_reward)
        print(query_time_reward)
        print(knn_time_reward)
        print(reward)
        print("$$$$$$$$$$$$$$$$$$$$$$")

        if reward > 0:
            reward = reward * 1000000

        return reward

    def _terminate(self):
        return self.terminate


class Server(OneDBEnv):
    """Build an environment directly on Server"""

    def __init__(self, wk_type):
        OneDBEnv.__init__(self, wk_type)
        self.wk_type = wk_type
        self.score = 0.0
        self.steps = 0
        self.terminate = False
        self.last_external_metrics = None

        self.db_info = configs.instance_config
        self.server_ip = self.db_info["host"]
        self.server_port = self.db_info["port"]
        self.alpha = 1.0
        knobs.init_knobs(num_more_knobs=0)
        self.default_knobs = knobs.get_init_knobs()

    def initialize(self):
        """Initialize the environment when an episode starts
        Returns:
            state: np.array, current state
        """
        self.score = 0.0
        self.last_external_metrics = []
        self.steps = 0
        self.terminate = False

        flag = self._apply_knobs(self.default_knobs)
        i = 0
        while not flag:
            flag = self._apply_knobs(self.default_knobs)
            i += 1
            if i >= 5:
                print("Initialize: {} times ....".format(i))

        # external_metrics, internal_metrics = self._get_state(
        #     knob=self.default_knobs, method=self.method
        # )
        external_metrics = (
            self.current_construct_time,
            self.current_query_time,
            self.current_knn_time,
        )
        internal_metrics = np.array(
            [
                self.current_construct_time,
                self.current_query_time,
                self.current_knn_time,
            ]
        )
        self.last_external_metrics = external_metrics
        self.default_externam_metrics = external_metrics
        state = internal_metrics
        knobs.save_knobs(
            self.default_knobs,
            metrics=external_metrics,
            knob_file="%ssave_knobs/knob_metric.txt" % PROJECT_DIR,
        )
        return state, external_metrics

    def _apply_knobs(self, knob):
        self.steps += 1
        response = utils.modify_configurations(
            server_ip=self.server_ip,
            server_port=self.server_port,
            configuration=knob,
        )

        # steps = 0
        # max_steps = 300
        # flag = utils.get_onedb_state(self.server_ip)
        # while not flag and steps < max_steps:
        #     flag = utils.get_onedb_state(self.server_ip)
        #     time.sleep(5)
        #     steps += 1
        if response:
            flag = True
            self.current_construct_time = response[0]
            self.current_query_time = response[1]
            self.current_knn_time = response[2]
        if not flag:
            params = ""
            for key in knob.keys():
                params += " --%s=%s" % (key, knob[key])
            with open("failed.log", "a+") as f:
                f.write("{}\n".format(params))
            return False
        else:
            return True

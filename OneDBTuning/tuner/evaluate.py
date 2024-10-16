# -*- coding: utf-8 -*-
"""
description: Evaluate the Model
"""

import os
import sys
import utils
import pickle
import argparse

sys.path.append("../")
import models
import environment
import numpy as np

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--params", type=str, required=True, help="Load existing model parameters"
)
parser.add_argument(
    "--workload",
    type=str,
    default="read",
    help="Type of workload [`read`, `write`, `readwrite`]",
)
parser.add_argument(
    "--method", type=str, default="ddpg", help="Algorithm to use [`ddpg`,`dqn`]"
)
parser.add_argument(
    "--replay_memory", type=str, default="", help="Path to replay memory file"
)
parser.add_argument(
    "--max_steps", type=int, default=50, help="Number of evaluation steps"
)
parser.add_argument(
    "--additional_knobs", type=int, default=0, help="Number of additional knobs"
)
parser.add_argument("--batch_size", type=int, default=2, help="Training batch size")
parser.add_argument("--num_metrics", type=int, default=65, help="Number of metrics")
options = parser.parse_args()

# Create environment based on options
if options.tencent:
    # If using a Tencent environment, set up different parameters
    server_env = environment.TencentServer(
        wk_type=options.workload,
        instance_name=options.instance,
        method=options.benchmark,
        num_metric=options.num_metrics,
        num_other_knobs=options.additional_knobs,
    )
else:
    # Otherwise, use a general environment
    server_env = environment.Server(
        wk_type=options.workload, instance_name=options.instance
    )

# Configure DDPG model parameters
ddpg_config = {
    "tau": 0.00001,  # Soft update parameter
    "actor_lr": 0.00001,  # Learning rate for the actor network
    "critic_lr": 0.00001,  # Learning rate for the critic network
    "model": options.params,  # Path to model parameters
}

# Configuration for state and action dimensions
num_states = options.num_metrics  # Number of state dimensions
gamma = 0.9  # Discount factor for future rewards
memory_capacity = 100000  # Capacity of replay memory
num_actions = 16 + options.additional_knobs  # Number of action dimensions
ddpg_config["gamma"] = gamma
ddpg_config["batch_size"] = options.batch_size
ddpg_config["memory_capacity"] = memory_capacity

# Initialize the DDPG model
ddpg_model = models.DDPG(
    n_states=num_states,
    n_actions=num_actions,
    opt=ddpg_config,
    use_ou_process=True,  # Use Ornstein-Uhlenbeck process for exploration
)

# Create directories for logs and test results if they don't exist
if not os.path.exists("log"):
    os.mkdir("log")

if not os.path.exists("test_knob"):
    os.mkdir("test_knob")

# Define experiment name
experiment_name = "eval_{}_{}".format(options.method, str(utils.get_timestamp()))

# Initialize logger
logger = utils.Logger(
    name=options.method, log_file="log/{}.log".format(experiment_name)
)

# Log warning if additional knobs are being used
if options.additional_knobs != 0:
    logger.warn("Using additional knobs in the environment configuration.")

# Get initial configuration of the environment
initial_knobs = environment.get_init_knobs()


def compute_metrics_percentage(default_metrics, current_metrics):
    """Compute the percentage change in metrics compared to default settings.
    Args:
        default_metrics: dict, metrics from the default settings
        current_metrics: dict, metrics from the current settings
    Returns:
        delta_tps, delta_latency: Percentage change in TPS and latency
    """
    delta_tps = 100 * (current_metrics[0] - default_metrics[0]) / default_metrics[0]
    delta_latency = 100 * (default_metrics[1] - current_metrics[1]) / default_metrics[1]
    return delta_tps, delta_latency


def generate_knob_configuration(action, algorithm):
    """Generate a configuration based on the algorithm.
    Args:
        action: Action values from the model
        algorithm: The algorithm used (e.g., DDPG)
    """
    if algorithm == "ddpg":
        return environment.gen_continuous(action)
    else:
        raise NotImplementedError("The specified algorithm is not implemented.")


# Load replay memory if provided
if len(options.replay_memory) > 0:
    ddpg_model.replay_memory.load_memory(options.replay_memory)
    print("Loaded Memory: {}".format(len(ddpg_model.replay_memory)))

# Initialize counters and other variables
step_count = 0
training_step_count = 0
accumulated_loss = [0, 0] if options.method == "ddpg" else 0
max_performance_score = 0
best_step_index = -1
collected_knobs = []
current_state, default_metrics = server_env.initialize()
ddpg_model.reset(0.1)

# Lists to record durations
step_duration_list = []
training_duration_list = []
environment_step_duration_list = []
restart_duration_list = []
action_selection_duration_list = []

print(
    "[Environment Initialized] TPS: {} Latency:{}".format(
        default_metrics[0], default_metrics[1]
    )
)
print("------------------- Starting Evaluation -----------------------")

# Evaluation loop
while step_count < options.max_steps:
    step_start_time = utils.time_start()

    state = current_state

    # Select action
    action_selection_start_time = utils.time_start()
    action = ddpg_model.choose_action(state)
    action_selection_duration = utils.time_end(action_selection_start_time)

    # Generate knob configuration
    current_knob_config = generate_knob_configuration(action, "ddpg")
    logger.info("[ddpg] Action: {}".format(action))

    # Apply configuration in the environment
    env_step_start_time = utils.time_start()
    reward, next_state, done, score, metrics, restart_duration = server_env.step(
        current_knob_config
    )
    env_step_duration = utils.time_end(env_step_start_time)

    logger.info(
        "[{}][Step: {}][Metrics: tps:{} lat:{} qps:{}] Reward: {} Score: {} Done: {}".format(
            options.method,
            step_count,
            metrics[0],
            metrics[1],
            metrics[2],
            reward,
            score,
            done,
        )
    )

    # Compute percentage change in TPS and latency
    tps_increase, latency_decrease = compute_metrics_percentage(
        default_metrics, metrics
    )

    logger.info(
        "[{}][Knob Step: {}] TPS increase: {}% Latency decrease: {}%".format(
            options.method, step_count, tps_increase, latency_decrease
        )
    )

    # Update the best score and index if current is better
    if tps_increase + latency_decrease > max_performance_score:
        max_performance_score = tps_increase + latency_decrease
        best_step_index = step_count

    # Add sample to replay memory
    next_state = next_state
    ddpg_model.add_sample(state, action, reward, next_state, done)

    # Collect generated knob configuration
    collected_knobs.append(
        {
            "tps_inc": tps_increase,
            "lat_dec": latency_decrease,
            "metrics": metrics,
            "knob": current_knob_config,
        }
    )

    # Save the collected knob configurations
    with open("test_knob/" + experiment_name + ".pkl", "wb") as file:
        pickle.dump(collected_knobs, file)

    current_state = next_state
    train_step_duration = 0.0
    if len(ddpg_model.replay_memory) >= options.batch_size:
        losses = []
        training_start_time = utils.time_start()
        for i in range(2):
            losses.append(ddpg_model.update())
            training_step_count += 1
        train_step_duration = utils.time_end(training_start_time) / 2.0

        # Record accumulated loss
        accumulated_loss[0] += sum([loss[0] for loss in losses])
        accumulated_loss[1] += sum([loss[1] for loss in losses])
        logger.info(
            "[{}][Step: {}] Critic: {} Actor: {}".format(
                options.method,
                step_count,
                accumulated_loss[0] / training_step_count,
                accumulated_loss[1] / training_step_count,
            )
        )

    # Record step duration
    step_duration = utils.time_end(step_start_time)
    step_duration_list.append(step_duration)
    environment_step_duration_list.append(env_step_duration)
    training_duration_list.append(train_step_duration)
    action_selection_duration_list.append(action_selection_duration)
    restart_duration_list.append(restart_duration)

    logger.info(
        "[{}][Step: {}] Total: {}s Env step: {}s Train step: {}s Restart: {}s Action: {}s".format(
            options.method,
            step_count,
            step_duration,
            env_step_duration,
            train_step_duration,
            restart_duration,
            action_selection_duration,
        )
    )

    logger.info(
        "[{}][Step: {}][Average] Total: {}s Env step: {}s Train step: {}s Restart: {}s Action: {}s".format(
            options.method,
            step_count,
            np.mean(step_duration_list),
            np.mean(environment_step_duration_list),
            np.mean(training_duration_list),
            np.mean(restart_duration_list),
            np.mean(action_selection_duration_list),
        )
    )

    step_count += 1

    # If the environment indicates the end of an episode, reinitialize
    if done:
        current_state, _ = server_env.initialize()
        ddpg_model.reset(0.01)

print("------------------- Evaluation Finished -----------------------")

print("Knobs are saved in: test_knob/" + experiment_name + ".pkl")
print("Log file is saved in: log/{}.log".format(experiment_name))

print("Proposal Knob At {}".format(best_step_index))

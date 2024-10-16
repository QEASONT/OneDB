# -*- coding: utf-8 -*-
"""
Train the model using the DDPG algorithm for server optimization.
"""

import os
import sys
import utils
import pickle
import argparse
sys.path.append('../')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import models
import numpy as np
import environment


def generate_configuration(action, algorithm):
    """Generates configuration settings based on the chosen algorithm."""
    if algorithm == 'ddpg':
        # For DDPG, generate continuous configuration values
        return environment.gen_continuous(action)
    else:
        raise NotImplementedError('The specified algorithm is not implemented')


if __name__ == '__main__':

    # Parse command-line arguments for various configurations
    parser = argparse.ArgumentParser(description="Train the DDPG model for server optimization.")
    parser.add_argument('--params', type=str, default='', help='Load existing model parameters')
    parser.add_argument('--workload', type=str, default='read', help='Type of workload [`read`, `write`, `readwrite`]')
    parser.add_argument('--algorithm', type=str, default='ddpg', help='Algorithm to use [`ddpg`, `dqn`]')
    parser.add_argument('--replay_memory', type=str, default='', help='Path to existing replay memory file')
    parser.add_argument('--use_noise', action='store_true', help='Enable noisy linear layers for exploration')
    parser.add_argument('--additional_knobs', type=int, default=0, help='Number of additional knobs')
    parser.add_argument('--batch_size', type=int, default=8, help='Training batch size')
    parser.add_argument('--epochs', type=int, default=1000000, help='Number of training epochs')
    parser.add_argument('--num_metrics', type=int, default=3, help='Number of metrics')
    parser.add_argument('--default_knobs', type=int, default=6, help='Number of default knobs')
    options = parser.parse_args()

    # Initialize and configure the environment based on the workload type
    server_environment = environment.Server(wk_type=options.workload)

    # Set up DDPG model options and parameters
    ddpg_config = {
        'tau': 0.00001,  # Soft update parameter for target network
        'actor_lr': 0.00001,  # Learning rate for the actor network
        'critic_lr': 0.00001,  # Learning rate for the critic network
        'model': options.params,  # Model parameters (if any) to load
        'gamma': 0.9,  # Discount factor for future rewards
        'batch_size': options.batch_size,  # Batch size for training
        'memory_capacity': 100000  # Capacity of the replay memory
    }

    state_count = options.num_metrics  # Number of metrics as state dimensions
    action_count = options.default_knobs + options.additional_knobs  # Total action dimensions

    # Initialize the DDPG model with specified state and action dimensions
    ddpg_model = models.DDPG(
        n_states=state_count,
        n_actions=action_count,
        opt=ddpg_config,
        mean_var_path='mean_var.pkl',  # Path for normalization parameters
        use_ou_process=not options.use_noise  # Whether to use Ornstein-Uhlenbeck process for noise
    )

    # Create necessary directories for logging and saving if they don't exist
    for folder in ['log', 'save_memory', 'save_knobs', 'save_state_actions', 'model_params']:
        if not os.path.exists(folder):
            os.mkdir(folder)

    # Define the experiment name for logging and saving files
    experiment_name = f'train_{options.algorithm}_{utils.get_timestamp()}'
    log_manager = utils.Logger(name=options.algorithm, log_file=f'log/{experiment_name}.log')

    # Log a warning if additional knobs are being used
    if options.additional_knobs != 0:
        log_manager.warn('Using additional knobs')

    # Get the initial configuration for the environment
    current_configuration = environment.get_init_knobs()

    # Training setup
    noise_decay_rate = 0.9  # Rate at which exploration noise decreases
    total_steps = 0  # Global step counter
    training_steps = 0  # Training step counter
    accumulated_loss = [0, 0]  # Accumulate loss for logging
    optimal_state_actions = []  # Store state-action pairs with high rewards

    # Load replay memory if specified
    if options.replay_memory:
        ddpg_model.replay_memory.load_memory(options.replay_memory)
        print(f"Loaded Memory: {len(ddpg_model.replay_memory)}")

    # Time tracking variables for performance analysis
    time_per_step = []
    time_per_training_step = []
    time_per_env_step = []
    time_per_restart = []
    time_per_action_selection = []

    for episode in range(options.epochs):
        # Initialize the environment and get the initial state and metrics
        current_state, initial_metrics = server_environment.initialize()
        log_manager.info(f"\n[Environment initialized][Metric tps: {initial_metrics[0]} lat: {initial_metrics[1]} qps: {initial_metrics[2]}]")
        
        noise_level = 0.1  # Initial noise level for exploration
        ddpg_model.reset(noise_level)  # Reset model with initial noise level
        step_count = 0  # Counter for steps in the current episode

        while True:
            # Record the start time of each step
            step_start_time = utils.time_start()
            state = current_state
            if options.use_noise:
                ddpg_model.sample_noise()  # Apply noise if enabled

            # Select an action based on the current state
            action_start_time = utils.time_start()
            selected_action = ddpg_model.choose_action(state)
            action_duration = utils.time_end(action_start_time)

            # Generate the configuration based on the action
            current_configuration = generate_configuration(selected_action, options.algorithm)
            log_manager.info(f"[{options.algorithm}] Action: {selected_action}")

            # Apply the configuration to the environment and observe the outcome
            env_start_time = utils.time_start()
            reward, next_state, done, score, metrics, restart_duration = server_environment.step(current_configuration)
            env_duration = utils.time_end(env_start_time)

            log_manager.info(f"\n[{options.algorithm}][Episode: {episode}][Step: {step_count}][Metric tps:{metrics[0]} lat:{metrics[1]} qps:{metrics[2]}] "
                             f"Reward: {reward} Score: {score} Done: {done}")
            time_per_restart.append(restart_duration)

            # Store the transition in replay memory
            ddpg_model.add_sample(state, selected_action, reward, next_state, done)

            # If reward is significant, store the state-action pair
            if reward > 10:
                optimal_state_actions.append((state, selected_action))

            current_state = next_state  # Update the state for the next step
            training_duration = 0.0
            if len(ddpg_model.replay_memory) > options.batch_size:
                # Update the model using samples from replay memory
                losses = []
                training_start_time = utils.time_start()
                for _ in range(2):
                    losses.append(ddpg_model.update())
                    training_steps += 1
                training_duration = utils.time_end(training_start_time) / 2.0

                # Accumulate and log losses
                accumulated_loss[0] += sum(loss[0] for loss in losses)
                accumulated_loss[1] += sum(loss[1] for loss in losses)
                log_manager.info(f"[{options.algorithm}][Episode: {episode}][Step: {step_count}] Critic: {accumulated_loss[0] / training_steps:.4f} "
                                 f"Actor: {accumulated_loss[1] / training_steps:.4f}")

            # Calculate and log time durations for various parts of the step
            step_duration = utils.time_end(step_start_time)
            time_per_step.append(step_duration)
            time_per_env_step.append(env_duration)
            time_per_training_step.append(training_duration)
            time_per_action_selection.append(action_duration)

            log_manager.info(f"[{options.algorithm}][Episode: {episode}][Step: {step_count}] step: {step_duration:.2f}s env step: {env_duration:.2f}s "
                             f"train step: {training_duration:.2f}s restart time: {restart_duration:.2f}s action time: {action_duration:.2f}s")

            log_manager.info(f"[{options.algorithm}][Episode: {episode}][Step: {step_count}][Average] step: {np.mean(time_per_step):.2f}s "
                             f"env step: {np.mean(time_per_env_step):.2f}s train step: {np.mean(time_per_training_step):.2f}s "
                             f"restart time: {np.mean(time_per_restart):.2f}s action time: {np.mean(time_per_action_selection):.2f}s")

            step_count += 1
            total_steps += 1

            # Save replay memory periodically
            if total_steps % 10 == 0:
                ddpg_model.replay_memory.save(f'save_memory/{experiment_name}.pkl')
                utils.save_state_actions(optimal_state_actions, f'save_state_actions/{experiment_name}.pkl')

            # Save network parameters periodically
            if total_steps % 5 == 0:
                ddpg_model.save_model('model_params', title=f'{experiment_name}_{total_steps}')

            # End episode if the environment signals 'done' or the score is too low
            if done or score < -50:
                break

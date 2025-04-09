FROM python:3.13.3-slim


RUN apt update \
    && apt upgrade -y \
    && apt install -y sqlite3 \
    && apt install -y git \
    && apt install -y make


# Create a non-root user with configurable UID and GID
ARG USERNAME=devuser
ARG USER_UID=1000
ARG USER_GID=1000

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update && apt-get install -y sudo \
    && echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN usermod --shell /bin/bash $USERNAME

# Switch to the non-root user
USER $USERNAME

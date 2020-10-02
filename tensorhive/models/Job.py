from sqlalchemy import Column, Integer, String, ForeignKey, Enum, DateTime, Text
from datetime import datetime
from tensorhive.database import Base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property
from tensorhive.models.CRUDModel import CRUDModel
from tensorhive.models.Task import Task, TaskStatus
from tensorhive.utils.DateUtils import DateUtils
from typing import Optional, Union
import enum
import logging
log = logging.getLogger(__name__)

class JobStatus(enum.Enum):
    not_running = 1
    running = 2
    terminated = 3
    unsynchronized = 4

class Job(CRUDModel, Base):  # type: ignore
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(40), unique=True, nullable=False)
    description = Column(Text)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.not_running, nullable=False)
    _start_at = Column(DateTime)
    _stop_at = Column(DateTime)

    _tasks = relationship(
        'Task', backref=backref('job', single_parent=True), lazy='subquery')


    def __repr__(self):
        return '<Job id={id}, name={name}, description={description}, user={user_id}, status={status}>'.format(
                id=self.id,
                name=self.name,
                description=self.description,
                user_id=self.user_id,
                status=self.status.name)

    def check_assertions(self):
        if self.stop_at is not None and self.start_at is not None:
            assert self.stop_at >= self.start_at, 'Time of the end must happen after the start!'
            assert self.stop_at > datetime.datetime.utcnow(), 'You are trying to edit time of the job that is already in the past'
        
        if self.stop_at is not None:
            assert self.start_at is not None, 'If stop time of the job is known, start time has to be known too'

    @hybrid_property
    def tasks(self):
        return self._tasks

    @hybrid_property
    def number_of_tasks(self):
        return len(self._tasks)

    def add_task(self, task: Task):
        if task in self.tasks:
            raise Exception('Task {task} is already assigned to job {job}!'
                                          .format(task=task, job=self))
        self.tasks.append(task)
        self.save()

    def remove_task(self, task: Task):
        if task not in self.tasks:
            raise Exception('Task {task} is not assigned to job {job}!'
                                          .format(task=task, job=self))
        self.tasks.remove(task)
        self.save()

    def synchronize_status(self, status: TaskStatus):
        """ Job status is synchronized on every change of one of its tasks status
        """
        for task in self.tasks:
            if task.status is not status:
                return
        if status is TaskStatus.unsynchronized:
            self.status = JobStatus.unsynchronized
        if status is TaskStatus.not_running:
            self.status = JobStatus.not_running
        if status is TaskStatus.terminated:
            self.status = JobStatus.terminated
        self.save()

    @hybrid_property
    def start_at(self):
        return self._start_at
    
    @hybrid_property
    def stop_at(self):
        return self._stop_at

    @property
    def as_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'userId': self.user_id,
            'status': self.status.name,
            'startAt': DateUtils.try_parse_datetime(self.start_at),
            'stopAt': DateUtils.try_parse_datetime(self.stop_at)
        }

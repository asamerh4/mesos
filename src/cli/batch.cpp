// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// mesos-batch command line tool (executes tasks from JSON file based on
// TaskGroupInfo proto)
// Code base derived from /src/cli/execute.cpp

#include <iostream>
#include <queue>
#include <vector>

#include <mesos/type_utils.hpp>

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <process/delay.hpp>
#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/protobuf.hpp>

#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>

#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"

#include "internal/devolve.hpp"

#include "v1/parse.hpp"

using std::cerr;
using std::cout;
using std::endl;
using std::queue;
using std::string;
using std::vector;

using google::protobuf::RepeatedPtrField;

using mesos::internal::devolve;

using mesos::v1::AgentID;
using mesos::v1::CapabilityInfo;
using mesos::v1::CommandInfo;
using mesos::v1::ContainerInfo;
using mesos::v1::Credential;
using mesos::v1::Environment;
using mesos::v1::ExecutorInfo;
using mesos::v1::FrameworkID;
using mesos::v1::FrameworkInfo;
using mesos::v1::Image;
using mesos::v1::Label;
using mesos::v1::Labels;
using mesos::v1::Offer;
using mesos::v1::Resource;
using mesos::v1::Resources;
using mesos::v1::TaskGroupInfo;
using mesos::v1::TaskID;
using mesos::v1::TaskInfo;
using mesos::v1::TaskState;
using mesos::v1::TaskStatus;
using mesos::v1::Volume;

using mesos::v1::scheduler::Call;
using mesos::v1::scheduler::Event;
using mesos::v1::scheduler::Mesos;

using process::Future;
using process::Owned;


class Flags : public virtual flags::FlagsBase
{
public:
  Flags()
  {
    add(&Flags::master,
        "master",
        "Mesos master (e.g., IP:PORT).");

    add(&Flags::task_list,
        "task_list",
        "The value could be a JSON-formatted string of `TaskGroupInfo` or a\n"
        "file path containing the JSON-formatted `TaskGroupInfo`. Path must\n"
        "be of the form `file:///path/to/file` or `/path/to/file`."
        "\n"
        "See the `TaskGroupInfo` message in `mesos.proto` for the expected\n"
        "format. NOTE: `agent_id` need not to be set.\n"
        "\n"
        "Example:\n"
        "{\n"
        "  \"tasks\": [{\n"
        "        \"name\": \"sub01-docker\",\n"
        "        \"task_id\": {\n"
        "           \"value\": \"sub01-docker\"\n"
        "        },\n"
        "        \"agent_id\": {\n"
        "           \"value\": \"\"\n"
        "        },\n"
        "        \"resources\": [{\n"
        "              \"name\": \"cpus\",\n"
        "              \"type\": \"SCALAR\",\n"
        "              \"scalar\": {\n"
        "                 \"value\": 2.5\n"
        "              }\n"
        "           }, {\n"
        "              \"name\": \"mem\",\n"
        "              \"type\": \"SCALAR\",\n"
        "              \"scalar\": {\n"
        "                 \"value\": 32\n"
        "              }\n"
        "           }\n"
        "        ],\n"
        "        \"command\": {\n"
        "           \"value\": \"ls -ltr && df\"\n"
        "        },\n"
        "        \"container\": {\n"
        "           \"type\": \"DOCKER\",\n"
        "           \"docker\": {\n"
        "              \"image\": \"alpine\"\n"
        "           }\n"
        "        }\n"
        "     },{...},{...},{...},{...}\n"
        "  ]\n"
        "}");

    add(&Flags::persistent_volume,
        "persistent_volume",
        "Enable dynamic reservation and creation of a persistent volume",
        false);

    add(&Flags::remove_persistent_volume,
        "remove_persistent_volume",
        "Unreserves dynamic reservations and removes persistent volumes\n"
        "if any\n",
        false);

    add(&Flags::persistent_volume_resource,
        "persistent_volume_resource",
        "The value could be a JSON formatted string of `TaskInfo` or a\n"
        "file path containing the JSON/protobuf. Path must\n"
        "be of the form `file:///path/to/file` or `/path/to/file`."
        "\n"
        "See the `TaskInfo` and the contained `Resource` messages\n"
        "in `mesos.proto` for the expected format.\n"
        "NOTE: `DiskInfo` inside the `disk` Resource must be present.\n"
        "\n"
        "Example:\n"
        "{\n"
        "  \"name\": \"persistent_vol_resource_spec\",\n"
        "  \"task_id\": {\n"
        "     \"value\": \"resource_spec001\"\n"
        "  },\n"
        "  \"agent_id\": {\n"
        "     \"value\": \"\"\n"
        "  },\n"
        "  \"resources\": [{\n"
        "       \"name\": \"cpus\",\n"
        "        \"type\": \"SCALAR\",\n"
        "        \"scalar\": {\n"
        "           \"value\": 3.0\n"
        "        },\n"
        "        \"role\": \"test\",\n"
        "        \"reservation\": {\n"
        "           \"principal\": \"test\"\n"
        "        }\n"
        "     }, {\n"
        "        \"name\": \"mem\",\n"
        "        \"type\": \"SCALAR\",\n"
        "        \"scalar\": {\n"
        "           \"value\": 64\n"
        "        },\n"
        "        \"role\": \"test\",\n"
        "        \"reservation\": {\n"
        "           \"principal\": \"test\"\n"
        "        }\n"
        "     }, {\n"
        "        \"name\": \"disk\",\n"
        "        \"type\": \"SCALAR\",\n"
        "        \"scalar\": {\n"
        "           \"value\": 4096\n"
        "        },\n"
        "        \"role\": \"test\",\n"
        "        \"reservation\": {\n"
        "           \"principal\": \"test\"\n"
        "        },\n"
        "        \"disk\": {\n"
        "           \"persistence\": {\n"
        "              \"id\": \"22d664c4-15d2-4978-86e3-d9b5d310666f\",\n"
        "              \"principal\": \"test\"\n"
        "           },\n"
        "           \"volume\": {\n"
        "              \"container_path\": \"volume\",\n"
        "              \"mode\": \"RW\"\n"
        "           }\n"
        "        }\n"
        "     }\n"
        "  ]\n"
        "}");

    add(&Flags::framework_name,
        "framework_name",
        "name of the framework",
        "mesos-batch instance");

    add(&Flags::checkpoint,
        "checkpoint",
        "Enable checkpointing for the framework.",
        false);

    add(&Flags::framework_capabilities,
        "framework_capabilities",
        "Comma-separated list of optional framework capabilities to enable.\n"
        "RESERVATION_REFINEMENT and TASK_KILLING_STATE are always enabled.\n"
        "PARTITION_AWARE is enabled unless --no-partition-aware is specified.");

    add(&Flags::role,
        "role",
        "Role to use when registering.",
        "*");

    add(&Flags::kill_after,
        "kill_after",
        "Specifies a delay after which the task is killed\n"
        "(e.g., 10secs, 2mins, etc).");

    add(&Flags::principal,
        "principal",
        "The principal to use for framework authentication.");

    add(&Flags::secret,
        "secret",
        "The secret to use for framework authentication.");

    add(&Flags::content_type,
        "content_type",
        "The content type to use for scheduler protocol messages. 'json'\n"
        "and 'protobuf' are valid choices.",
        "protobuf");
    add(&Flags::partition_aware,
        "partition_aware",
        "Enable partition-awareness for the framework.",
        true);
  }

  string master;
  string framework_name;
  Option<TaskGroupInfo> task_list;
  bool persistent_volume;
  bool remove_persistent_volume;
  Option<TaskInfo> persistent_volume_resource;
  bool checkpoint;
  Option<std::set<string>> framework_capabilities;
  string role;
  Option<Duration> kill_after;
  Option<string> principal;
  Option<string> secret;
  string content_type;
  bool partition_aware;
};

class CommandScheduler : public process::Process<CommandScheduler>
{
public:
  CommandScheduler(
      const FrameworkInfo& _frameworkInfo,
      const string& _master,
      const string& _role,
      mesos::ContentType _contentType,
      const Option<Duration>& _killAfter,
      const Option<Credential>& _credential,
      const Option<TaskGroupInfo>& _taskGroup,
      const bool& _persistentVolume,
      const Option<TaskInfo>& _persistentVolumeResource)
    : state(DISCONNECTED),
      frameworkInfo(_frameworkInfo),
      master(_master),
      role(_role),
      contentType(_contentType),
      killAfter(_killAfter),
      credential(_credential),
      taskGroup(_taskGroup),
      persistentVolume(_persistentVolume),
      persistentVolumeResource(_persistentVolumeResource),
      persistentVolumeReserved(false),
      persistentVolumeCreated(false),
      terminatedTaskCount(0),
      dTasksLaunched(0) {}

  virtual ~CommandScheduler() {}

  // Vector for reporting failed tasks.
  vector<mesos::v1::TaskID> failedTasks;

  // Vector holding task_list.
  vector<TaskInfo> tasks;
  // Vector holding persistent volume infos, but without DiskInfo.
  vector<Resource> plainResources;
  // Vector holding all persistent volume infos (including DiskInfo)
  vector<Resource> disk;

protected:
  virtual void initialize()
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    mesos.reset(new Mesos(
      master,
      contentType,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1),
      credential));

    // Fill up tasks vector from task_list.
    foreach (TaskInfo _task, taskGroup->tasks()) {
      tasks.push_back(_task);
    }

    // Prepare vectors plainResource & disk
    if (persistentVolume) {
        foreach (Resource _resource, persistentVolumeResource->resources()){
          if(_resource.name() == "disk"){
            Resource plain;
            plain.set_name(_resource.name());
            plain.set_type(_resource.type());
            plain.mutable_scalar()->CopyFrom(_resource.scalar());
            // plain.set_role(_resource.role());
            plain.mutable_reservations()->CopyFrom(_resource.reservations());
            // Push to plainResources
            plainResources.push_back(plain);
            // Push to disk
            disk.push_back(_resource);
          }
          else {
            plainResources.push_back(_resource);
          }
        }
    }
  }

  void connected()
  {
    state = CONNECTED;

    doReliableRegistration();
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void doReliableRegistration()
  {
    if (state == SUBSCRIBED || state == DISCONNECTED) {
      return;
    }

    Call call;
    call.set_type(Call::SUBSCRIBE);

    if (frameworkInfo.has_id()) {
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());
    }

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(frameworkInfo);

    mesos->send(call);

    process::delay(Seconds(1), self(), &Self::doReliableRegistration);
  }

  void killTask(const TaskID& taskId, const AgentID& agentId)
  {
    cout << "Asked to kill task '" << taskId
         << "' on agent '" << agentId << "'" << endl;

    Call call;
    call.set_type(Call::KILL);

    CHECK(frameworkInfo.has_id());
    call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

    Call::Kill* kill = call.mutable_kill();
    kill->mutable_task_id()->CopyFrom(taskId);
    kill->mutable_agent_id()->CopyFrom(agentId);

    mesos->send(call);
  }

  void offers(const vector<Offer>& offers)
  {
    CHECK_EQ(SUBSCRIBED, state);
    // Loop all offers and place tasks...
    foreach (const Offer& offer, offers) {
      Resources offered = offer.resources();
      // Strip the allocation of all derived resources from offer (MESOS-7075)
      offered.unallocate();
      Resources reserved = offered.reserved(role);
      reserved.unallocate();
      // Resources needed for tasks.
      Resources requiredResources;
      // Container for runnable tasks within current offer
      vector<TaskInfo> runnable_tasks;

      cout << "Received offer from agent "
           << offer.hostname()
           << " with: " << offer.resources() << endl;

      // Check if we need a new dynamic reservation.
      if (persistentVolume) {
        if (reserved.contains(Resources(plainResources))){
          persistentVolumeReserved = true;
        }
        if(!persistentVolumeReserved &&
             !persistentVolumeCreated &&
             offered.contains(Resources(plainResources).toUnreserved())){
          cout << "Requested reserved resources: "
               << plainResources
               << " for -> **"
               << offer.hostname()
               << endl;
          Call call;
          call.set_type(Call::ACCEPT);

          CHECK(!reserved.contains(Resources(plainResources)));
          CHECK(frameworkInfo.has_id());

          call.mutable_framework_id()->CopyFrom(frameworkInfo.id());
          Call::Accept* accept = call.mutable_accept();
          accept->add_offer_ids()->CopyFrom(offer.id());

          // Reserve Operation
          Offer::Operation* operation = accept->add_operations();
          operation->set_type(Offer::Operation::RESERVE);

          operation->mutable_reserve()->mutable_resources()
            ->CopyFrom(Resources(plainResources));

          // Create Operation
          Offer::Operation* operation2 = accept->add_operations();
          operation2->set_type(Offer::Operation::CREATE);
          operation2->mutable_create()->mutable_volumes()
            ->CopyFrom(Resources(disk));

          // Send the call including RESERVE and CREATE operations
          mesos->send(call);

          persistentVolumeReserved = true;
          persistentVolumeCreated = true;
          cout << "Resources reserved & Volume created using "
               << plainResources
               << " from -> "
               << offer.hostname()
               << endl;
        }
      }
      // Iterate over TaskGroupInfo content and push to runnable_tasks
      while (dTasksLaunched < (int) tasks.size()) {
          if (persistentVolume && persistentVolumeReserved){
            TaskInfo _task = tasks[dTasksLaunched];
            requiredResources = Resources(_task.resources());

              if (reserved.contains(requiredResources)) {
                  _task.mutable_agent_id()->MergeFrom(offer.agent_id());
                  _task.mutable_resources()->CopyFrom(requiredResources);
                  runnable_tasks.push_back(_task);
                  dTasksLaunched++;
              }
              else {
                  break;
              }
              reserved -= requiredResources;
          }
          else {
              TaskInfo _task = tasks[dTasksLaunched];
              requiredResources = Resources(_task.resources());

              if (offered.contains(requiredResources)) {
                  _task.mutable_agent_id()->MergeFrom(offer.agent_id());
                  _task.mutable_resources()->CopyFrom(requiredResources);
                  runnable_tasks.push_back(_task);
                  dTasksLaunched++;
              }
              else {
                  break;
              }
              offered -= requiredResources;
        }
      }
      Call call;
      call.set_type(Call::ACCEPT);

      CHECK(frameworkInfo.has_id());
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

      Call::Accept* accept = call.mutable_accept();
      accept->add_offer_ids()->CopyFrom(offer.id());

      Offer::Operation* operation = accept->add_operations();

      operation->set_type(Offer::Operation::LAUNCH);
      foreach (const TaskInfo& task, runnable_tasks) {
         operation->mutable_launch()->add_task_infos()->CopyFrom(task);
        }
      mesos->send(call);
    }
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          frameworkInfo.mutable_id()->
            CopyFrom(event.subscribed().framework_id());

          state = SUBSCRIBED;

          cout << "Subscribed batch framework: \033[1;33m**"
               << frameworkInfo.name()
               << "-->" << frameworkInfo.id() << "\033[0m" << endl;
          break;
        }

        case Event::OFFERS: {
          offers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::UPDATE: {
          update(event.update().status());
          break;
        }

        case Event::ERROR: {
          EXIT(EXIT_FAILURE)
            << "Received an ERROR event: " << event.error().message();

          break;
        }

        case Event::HEARTBEAT:
        case Event::INVERSE_OFFERS:
        case Event::FAILURE:
        case Event::RESCIND:
        case Event::RESCIND_INVERSE_OFFER:
        case Event::UPDATE_OPERATION_STATUS:
        case Event::MESSAGE: {
          break;
        }

        case Event::UNKNOWN: {
          LOG(WARNING) << "Received an UNKNOWN event and ignored";
          break;
        }
      }
    }
  }

  void update(const TaskStatus& status)
  {
    CHECK_EQ(SUBSCRIBED, state);

    cout << "Received status update " << status.state()
         << " for task '" << status.task_id() << "'" << endl;

    if (status.has_message()) {
      cout << "  message: '" << status.message() << "'" << endl;
    }
    if (status.has_source()) {
      cout << "  source: " << TaskStatus::Source_Name(status.source()) << endl;
    }
    if (status.has_reason()) {
      cout << "  reason: " << TaskStatus::Reason_Name(status.reason()) << endl;
    }
    if (status.has_healthy()) {
      cout << "  healthy?: " << status.healthy() << endl;
    }

    if (status.has_check_status()) {
      cout << "  check status: " << status.check_status() << endl;
    }

    if (status.has_uuid()) {
      Call call;
      call.set_type(Call::ACKNOWLEDGE);

      CHECK(frameworkInfo.has_id());
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

      Call::Acknowledge* acknowledge = call.mutable_acknowledge();
      acknowledge->mutable_agent_id()->CopyFrom(status.agent_id());
      acknowledge->mutable_task_id()->CopyFrom(status.task_id());
      acknowledge->set_uuid(status.uuid());

      mesos->send(call);
    }

    // If a task kill delay has been specified, schedule task kill.
    if (killAfter.isSome() && TaskState::TASK_RUNNING == status.state()) {
      delay(killAfter.get(),
            self(),
            &Self::killTask,
            status.task_id(),
            status.agent_id());
    }

    if (mesos::internal::protobuf::isTerminalState(devolve(status).state())) {
        terminatedTaskCount++;
        if (terminatedTaskCount == taskGroup->tasks().size()) {
          terminate(self());
        }
    }

    if (status.state() == TaskState::TASK_FAILED ||
         status.state() == TaskState::TASK_LOST ||
         status.state() == TaskState::TASK_KILLED){
       failedTasks.push_back(status.task_id());
    }
  }

private:
  enum State
  {
    DISCONNECTED,
    CONNECTED,
    SUBSCRIBED
  } state;

  FrameworkInfo frameworkInfo;
  const string master;
  const string role;
  mesos::ContentType contentType;
  const Option<Duration> killAfter;
  const Option<Credential> credential;
  const Option<TaskGroupInfo> taskGroup;
  bool persistentVolume;
  const Option<TaskInfo> persistentVolumeResource;
  bool persistentVolumeReserved;
  bool persistentVolumeCreated;
  int terminatedTaskCount;
  int dTasksLaunched;
  Owned<Mesos> mesos;
};

class UnreserveScheduler : public process::Process<UnreserveScheduler>
{
public:
  UnreserveScheduler(
      const FrameworkInfo& _frameworkInfo,
      const string& _master,
      const string& _role,
      mesos::ContentType _contentType,
      const Option<Credential>& _credential,
      const Option<TaskInfo>& _persistentVolumeResource)
    : state(DISCONNECTED),
      frameworkInfo(_frameworkInfo),
      master(_master),
      role(_role),
      contentType(_contentType),
      credential(_credential),
      persistentVolumeResource(_persistentVolumeResource){}

  virtual ~UnreserveScheduler() {}

  // Vector holding persistent volume infos, but without DiskInfo.
  vector<Resource> plainResources;
  // Vector holding all persistent volume infos (including DiskInfo)
  vector<Resource> disk;

protected:
  virtual void initialize()
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    mesos.reset(new Mesos(
      master,
      contentType,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1),
      credential));

    foreach (Resource _resource, persistentVolumeResource->resources()) {
      if(_resource.name() == "disk"){
        Resource plain;
        plain.set_name(_resource.name());
        plain.set_type(_resource.type());
        plain.mutable_scalar()->CopyFrom(_resource.scalar());
        // plain.set_role(_resource.role());
        plain.mutable_reservations()->CopyFrom(_resource.reservations());
        plainResources.push_back(plain);
        // Push to disk
        disk.push_back(_resource);
      }
      else {
        plainResources.push_back(_resource);
      }
    }
  }

  void connected()
  {
    state = CONNECTED;

    doReliableRegistration();
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void doReliableRegistration()
  {
    if (state == SUBSCRIBED || state == DISCONNECTED) {
      return;
    }

    Call call;
    call.set_type(Call::SUBSCRIBE);

    if (frameworkInfo.has_id()) {
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());
    }

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(frameworkInfo);

    mesos->send(call);

    process::delay(Seconds(1), self(), &Self::doReliableRegistration);
  }

  void offers(const vector<Offer>& offers)
  {
    CHECK_EQ(SUBSCRIBED, state);

    // Vector holding declined offers for checking terminal state (DIRTY)
    // Use state machine approach in the future...
    vector<Offer> declinedOffers;

    // loop all offers and place DESTROY and UNRESERVE Operation
    foreach (const Offer& offer, offers) {
      Resources offered = offer.resources();
      // Strip the allocation of all resources derived from `offer`(MESOS-7075)
      offered.unallocate();
      Resources reserved = offered.reserved(role);
      reserved.unallocate();
      Resources created = offered.persistentVolumes();
      created.unallocate();

      cout << "Received offer from agent "
           << offer.hostname()
           << " with: " << offer.resources() << endl;

      if (!reserved.contains(Resources(plainResources))
           && !created.contains(Resources(disk))){
        cout << "Nothing to unreserve..." << endl;

        // At the end of the offer cycle this vector must be
        // of the same size as `offers`
        declinedOffers.push_back(offer);
        Call declineCall;
        declineCall.set_type(Call::DECLINE);

        CHECK(frameworkInfo.has_id());

        declineCall.mutable_framework_id()->CopyFrom(frameworkInfo.id());
        Call::Decline* decline = declineCall.mutable_decline();
        decline->add_offer_ids()->CopyFrom(offer.id());
        mesos->send(declineCall);
      }
      else {
        Call call;
        call.set_type(Call::ACCEPT);

        CHECK(frameworkInfo.has_id());

        call.mutable_framework_id()->CopyFrom(frameworkInfo.id());
        Call::Accept* accept = call.mutable_accept();
        accept->add_offer_ids()->CopyFrom(offer.id());

        // Destroy Operation
        Offer::Operation* operation2 = accept->add_operations();
        operation2->set_type(Offer::Operation::DESTROY);
        operation2->mutable_destroy()->mutable_volumes()
          ->CopyFrom(Resources(disk));

        // Unreserve Operation
        Offer::Operation* operation = accept->add_operations();
        operation->set_type(Offer::Operation::UNRESERVE);

        operation->mutable_unreserve()
          ->mutable_resources()
          ->CopyFrom(Resources(plainResources));
        cout << "Destroyed and unreserved: " << plainResources
             << endl;

        // Send the call including DESTROY and UNRESERVE operations
        mesos->send(call);
      }
    }

    // Check if we reached a terminal state
    if(offers.size() == declinedOffers.size()){
      terminate(self());
    }
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          frameworkInfo.mutable_id()->
            CopyFrom(event.subscribed().framework_id());

          state = SUBSCRIBED;

          cout << "Subscribed unreserve-framework: \033[1;33m**"
               << frameworkInfo.name()
               << "-->" << frameworkInfo.id() << "\033[0m" << endl;
          break;
        }

        case Event::OFFERS: {
          offers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::ERROR: {
          EXIT(EXIT_FAILURE)
            << "Received an ERROR event: " << event.error().message();

          break;
        }

        case Event::UPDATE:
        case Event::HEARTBEAT:
        case Event::INVERSE_OFFERS:
        case Event::FAILURE:
        case Event::RESCIND:
        case Event::RESCIND_INVERSE_OFFER:
        case Event::UPDATE_OPERATION_STATUS:
        case Event::MESSAGE: {
          break;
        }

        case Event::UNKNOWN: {
          LOG(WARNING) << "Received an UNKNOWN event and ignored";
          break;
        }
      }
    }
  }

private:
  enum State
  {
    DISCONNECTED,
    CONNECTED,
    SUBSCRIBED
  } state;

  FrameworkInfo frameworkInfo;
  const string master;
  const string role;
  mesos::ContentType contentType;
  const Option<Credential> credential;
  const Option<TaskInfo> persistentVolumeResource;
  Owned<Mesos> mesos;
};


int main(int argc, char** argv)
{
cout << R"( ___  ___  ___
|\__\|\__\|\__\  commandline batch processing framework for mesos 1.5++
\|__|\|__|\|__|  github.com/asamerh4/mesos-batch

)";

  Flags flags;
  mesos::ContentType contentType = mesos::ContentType::PROTOBUF;

  // Load flags from command line only.
  Try<flags::Warnings> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  // Log any flag warnings.
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  if (flags.content_type == "json" ||
      flags.content_type == mesos::APPLICATION_JSON) {
    contentType = mesos::ContentType::JSON;
  } else if (flags.content_type == "protobuf" ||
             flags.content_type == mesos::APPLICATION_PROTOBUF) {
    contentType = mesos::ContentType::PROTOBUF;
  } else {
    cerr << "Invalid content type '" << flags.content_type << "'" << endl;
    return EXIT_FAILURE;
  }

  Result<string> user = os::user();
  if (!user.isSome()) {
    if (user.isError()) {
      cerr << "Failed to get username: " << user.error() << endl;
    } else {
      cerr << "No username for uid " << endl;
    }
    return EXIT_FAILURE;
  }

  // Always enable the TASK_KILLING_STATE capability.
  vector<FrameworkInfo::Capability::Type> frameworkCapabilities = {
    FrameworkInfo::Capability::RESERVATION_REFINEMENT,
    FrameworkInfo::Capability::TASK_KILLING_STATE,
    FrameworkInfo::Capability::REVOCABLE_RESOURCES,
  };

  // Enable PARTITION_AWARE unless disabled by the user.
  if (flags.partition_aware) {
    frameworkCapabilities.push_back(
        FrameworkInfo::Capability::PARTITION_AWARE);
  }

  if (flags.framework_capabilities.isSome()) {
    foreach (const string& capability, flags.framework_capabilities.get()) {
      FrameworkInfo::Capability::Type type;

      if (!FrameworkInfo::Capability::Type_Parse(capability, &type)) {
        cerr << "Flags '--framework_capabilities'"
                " specifies an unknown capability"
                " '" << capability << "'" << endl;
        return EXIT_FAILURE;
      }
      frameworkCapabilities.push_back(type);
    }
  }

  FrameworkInfo frameworkInfo;
  frameworkInfo.set_user(user.get());
  frameworkInfo.set_name(flags.framework_name);
  frameworkInfo.set_role(flags.role);
  frameworkInfo.set_checkpoint(flags.checkpoint);
  foreach (const FrameworkInfo::Capability::Type& capability,
           frameworkCapabilities) {
    frameworkInfo.add_capabilities()->set_type(capability);
  }

  Option<Credential> credential = None();

  if (flags.principal.isSome()) {
    frameworkInfo.set_principal(flags.principal.get());

    if (flags.secret.isSome()) {
      Credential credential_;
      credential_.set_principal(flags.principal.get());
      credential_.set_secret(flags.secret.get());
      credential = credential_;
    }
  }

  if (flags.task_list.isNone()) {
    if(flags.remove_persistent_volume){
     // empty
    }
    else{
      cerr
      << "No protobuf/json message for '--task_list' provided."
      << endl;
      cout << flags.usage() << endl;
      return EXIT_FAILURE;
    }
  }

  // Check optional persistent volume info
  if (flags.persistent_volume_resource.isNone() && flags.persistent_volume) {
    cerr
    << "No protobuf/json message for '--persistent_volume_resource' provided."
    << endl;
    cout << flags.usage() << endl;
    return EXIT_FAILURE;
  }

  if (flags.persistent_volume_resource.isNone()
    && flags.remove_persistent_volume) {
    cerr
    << "No protobuf/json message for '--persistent_volume_resource' provided."
    << endl;
    cout << flags.usage() << endl;
    return EXIT_FAILURE;
  }

  if (flags.persistent_volume_resource.isSome()
      && flags.persistent_volume
      && flags.remove_persistent_volume) {
      cerr << "Flags 'persistent_volume' AND 'remove_persistent_volume'\n"
              "were specified. Please use these flags exclusively"
      << endl;
      cout << flags.usage() << endl;
      return EXIT_FAILURE;
  }

  if((flags.persistent_volume || flags.remove_persistent_volume)
      && (flags.principal.isNone() || flags.role == "")){
    cerr
      << "Operations on reserved resources require valid flags\n"
         "for 'role' AND 'principal'"
      << endl;
    cout << flags.usage() << endl;
    return EXIT_FAILURE;
  }

  if(flags.remove_persistent_volume){
      frameworkInfo.set_name("UNRESERVE_RESOURCES-"+flags.framework_name);
      Owned<UnreserveScheduler> scheduler2(
      new UnreserveScheduler(
        frameworkInfo,
        flags.master,
        flags.role,
        contentType,
        credential,
        flags.persistent_volume_resource));

    process::spawn(scheduler2.get());
    process::wait(scheduler2.get());

    cout << "Unsubscribed unreserve-framework: "
         << frameworkInfo.name()
         << endl;
    return EXIT_SUCCESS;
  }
  else {
    Owned<CommandScheduler> scheduler(
      new CommandScheduler(
        frameworkInfo,
        flags.master,
        flags.role,
        contentType,
        flags.kill_after,
        credential,
        flags.task_list,
        flags.persistent_volume,
        flags.persistent_volume_resource));

    process::spawn(scheduler.get());
    process::wait(scheduler.get());

    cout << "Unsubscribed batch framework: "
         << frameworkInfo.name()
         << endl;
    /* Use curl master.mesos:5050/frameworks |
    jq --compact-output '.[] | .[] | select(.id=="ID")' for JSON stats */

    // Report failed tasks if any.
    foreach (const mesos::v1::TaskID& failedTaskId, scheduler->failedTasks) {
      cout << "**failed task-->" << failedTaskId << " with command: '";
      foreach (const TaskInfo& task, scheduler->tasks){
        if(task.task_id() == failedTaskId && task.has_command()){
          cout << task.command().value() << "'" << endl;
        }
      }
    }
    if(scheduler->failedTasks.size() > 0){
      return EXIT_FAILURE;
    }
    else {
      return EXIT_SUCCESS;
    }
  }
}

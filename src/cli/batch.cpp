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

//mesos-batch command line tool (executes tasks from JSON file based on TaskGroupInfo proto)
//Code base derived from https://github.com/apache/mesos/blob/master/src/cli/execute.cpp

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
        "  \"tasks\":\n"
        "     [\n"
        "        {\n"
        "         \"name\": \"Name of the task\",\n"
        "         \"task_id\": {\"value\" : \"Id of the task\"},\n"
        "         \"agent_id\": {\"value\" : \"\"},\n"
        "         \"resources\": [{\n"
        "            \"name\": \"cpus\",\n"
        "            \"type\": \"SCALAR\",\n"
        "            \"scalar\": {\n"
        "                \"value\": 0.1\n"
        "             },\n"
        "            \"role\": \"*\"\n"
        "           },\n"
        "           {\n"
        "            \"name\": \"mem\",\n"
        "            \"type\": \"SCALAR\",\n"
        "            \"scalar\": {\n"
        "                \"value\": 32\n"
        "             },\n"
        "            \"role\": \"*\"\n"
        "          }],\n"
        "         \"command\": {\n"
        "            \"value\": \"sleep 1000\"\n"
        "           }\n"
        "       }\n"
        "     ]\n"
        "}");

    
    add(&Flags::framework_name,
        "framework_name",
        "name of the framework",
        "mesos-execute instance");

    add(&Flags::checkpoint,
        "checkpoint",
        "Enable checkpointing for the framework.",
        false);

    add(&Flags::framework_capabilities,
        "framework_capabilities",
        "Comma separated list of optional framework capabilities to enable.\n"
        "(the only valid value is currently 'GPU_RESOURCES')");

    add(&Flags::capabilities,
        "capabilities",
        "JSON representation of system capabilities needed to execute \n"
        "the command.\n"
        "Example:\n"
        "{\n"
        "   \"capabilities\": [\n"
        "       \"NET_RAW\",\n"
        "       \"SYS_ADMIN\"\n"
        "     ]\n"
        "}");

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

  }

  string master;
  string framework_name;
  Option<TaskGroupInfo> task_list;
  bool checkpoint;
  Option<std::set<string>> framework_capabilities;
  Option<CapabilityInfo> capabilities;
  string role;
  Option<Duration> kill_after;
  Option<string> principal;
  Option<string> secret;
};


class CommandScheduler : public process::Process<CommandScheduler>
{
public:
  CommandScheduler(
      const FrameworkInfo& _frameworkInfo,
      const string& _master,
      const Option<Duration>& _killAfter,
      const Option<Credential>& _credential,
      const Option<TaskGroupInfo>& _taskGroup)
    : state(DISCONNECTED),
      frameworkInfo(_frameworkInfo),
      master(_master),
      killAfter(_killAfter),
      credential(_credential),
      taskGroup(_taskGroup),
      launched(false),
      terminatedTaskCount(0),
      dTasksLaunched(0) {}

  virtual ~CommandScheduler() {}

protected:
  virtual void initialize()
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    mesos.reset(new Mesos(
      master,
      mesos::ContentType::PROTOBUF,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1),
      credential));
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
    //loop all offers and place tasks...
    foreach (const Offer& offer, offers) {
      Resources offered = offer.resources();
      Resources requiredResources;
        
        
      cout << "Received offer " << offer.id() << " from agent "
          << offer.agent_id() << " (" << offer.hostname() << ") "
          << "with " << offer.resources() << endl;
      vector<TaskInfo> tasks;
      vector<TaskInfo> runnable_tasks;

      foreach (TaskInfo _task, taskGroup->tasks()) {
          tasks.push_back(_task);
      }
      //Iterate over TaskGroupInfo content and push to runnable_tasks
      while (dTasksLaunched < (int) tasks.size()) {
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

          cout << "Subscribed with ID " << frameworkInfo.id() << endl;
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
  const Option<Duration> killAfter;
  const Option<Credential> credential;
  const Option<TaskGroupInfo> taskGroup;
  bool launched;
  int terminatedTaskCount;
  int dTasksLaunched;
  Owned<Mesos> mesos;
};


int main(int argc, char** argv)
{
  Flags flags;
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

  Result<string> user = os::user();
  if (!user.isSome()) {
    if (user.isError()) {
      cerr << "Failed to get username: " << user.error() << endl;
    } else {
      cerr << "No username for uid " << ::getuid() << endl;
    }
    return EXIT_FAILURE;
  }

  // We set the TASK_KILLING_STATE capability by default.
  vector<FrameworkInfo::Capability::Type> frameworkCapabilities =
    { FrameworkInfo::Capability::TASK_KILLING_STATE };

  if (flags.framework_capabilities.isSome()) {
    foreach (const string& capability, flags.framework_capabilities.get()) {
      FrameworkInfo::Capability::Type type;

      if (!FrameworkInfo::Capability::Type_Parse(capability, &type)) {
        cerr << "Flags '--framework_capabilities'"
                " specifes an unknown capability"
                " '" << capability << "'" << endl;
        return EXIT_FAILURE;
      }

      if (type != FrameworkInfo::Capability::GPU_RESOURCES) {
        cerr << "Flags '--framework_capabilities'"
                " specifes an unsupported capability"
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

  Option<TaskInfo> taskInfo = None();

  if (flags.task_list.isNone()) {
      cout << flags.usage() << endl;
      return EXIT_FAILURE;
    
  }

  Owned<CommandScheduler> scheduler(
      new CommandScheduler(
        frameworkInfo,
        flags.master,
        flags.kill_after,
        credential,
        flags.task_list));

  process::spawn(scheduler.get());
  process::wait(scheduler.get());

  return EXIT_SUCCESS;
}

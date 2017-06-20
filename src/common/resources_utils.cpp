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

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/stringify.hpp>

#include "common/resources_utils.hpp"

namespace mesos {

bool needCheckpointing(const Resource& resource)
{
  return Resources::isDynamicallyReserved(resource) ||
         Resources::isPersistentVolume(resource);
}


// NOTE: We effectively duplicate the logic in 'Resources::apply'
// which is less than ideal. But we cannot simply create
// 'Offer::Operation' and invoke 'Resources::apply' here.
// 'RESERVE' operation requires that the specified resources are
// dynamically reserved only, and 'CREATE' requires that the
// specified resources are already dynamically reserved.
// These requirements are violated when we try to infer dynamically
// reserved persistent volumes.
// TODO(mpark): Consider introducing an atomic 'RESERVE_AND_CREATE'
// operation to solve this problem.
Try<Resources> applyCheckpointedResources(
    const Resources& resources,
    const Resources& checkpointedResources)
{
  Resources totalResources = resources;

  foreach (const Resource& resource, checkpointedResources) {
    if (!needCheckpointing(resource)) {
      return Error("Unexpected checkpointed resources " + stringify(resource));
    }

    Resource stripped = resource;

    if (Resources::isDynamicallyReserved(resource)) {
      stripped.set_role("*");
      stripped.clear_reservation();
    }

    // Strip persistence and volume from the disk info so that we can
    // check whether it is contained in the `totalResources`.
    if (Resources::isPersistentVolume(resource)) {
      if (stripped.disk().has_source()) {
        stripped.mutable_disk()->clear_persistence();
        stripped.mutable_disk()->clear_volume();
      } else {
        stripped.clear_disk();
      }
    }

    stripped.clear_shared();

    if (!totalResources.contains(stripped)) {
      return Error(
          "Incompatible agent resources: " + stringify(totalResources) +
          " does not contain " + stringify(stripped));
    }

    totalResources -= stripped;
    totalResources += resource;
  }

  return totalResources;
}


void convertResourceFormat(Resource* resource, ResourceFormat format)
{
  switch (format) {
    case PRE_RESERVATION_REFINEMENT:
    case ENDPOINT: {
      CHECK(!resource->has_role());
      CHECK(!resource->has_reservation());
      switch (resource->reservations_size()) {
        // Unreserved resource.
        case 0: {
          resource->set_role("*");
          break;
        }
        // Resource with a single reservation.
        case 1: {
          const Resource::ReservationInfo& source = resource->reservations(0);

          if (source.type() == Resource::ReservationInfo::DYNAMIC) {
            Resource::ReservationInfo* target = resource->mutable_reservation();
            if (source.has_principal()) {
              target->set_principal(source.principal());
            }
            if (source.has_labels()) {
              target->mutable_labels()->CopyFrom(source.labels());
            }
          }

          resource->set_role(source.role());

          if (format == PRE_RESERVATION_REFINEMENT) {
            resource->clear_reservations();
          }
          break;
        }
        // Resource with refined reservations.
        default: {
          CHECK_NE(PRE_RESERVATION_REFINEMENT, format)
            << "Invalid resource format conversion: A 'Resource' object"
               " being converted to the PRE_RESERVATION_REFINEMENT format"
               " must not have refined reservations";
        }
      }
      break;
    }
    case POST_RESERVATION_REFINEMENT: {
      if (resource->reservations_size() > 0) {
        // In this case, we're either already in
        // the "post-reservation-refinement" format,
        // or we're in the "endpoint" format.

        // We clear out the "pre-reservation-refinement" fields
        // in case the resources are in the "endpoint" format.
        resource->clear_role();
        resource->clear_reservation();
        return;
      }

      // Unreserved resources.
      if (resource->role() == "*" && !resource->has_reservation()) {
        resource->clear_role();
        return;
      }

      // Resource with a single reservation.
      Resource::ReservationInfo* reservation = resource->add_reservations();

      // Check the `Resource.reservation` to determine whether
      // we have a static or dynamic reservation.
      if (!resource->has_reservation()) {
        reservation->set_type(Resource::ReservationInfo::STATIC);
      } else {
        reservation->CopyFrom(resource->reservation());
        resource->clear_reservation();
        reservation->set_type(Resource::ReservationInfo::DYNAMIC);
      }

      reservation->set_role(resource->role());
      resource->clear_role();
      break;
    }
  }
}


void convertResourceFormat(
    google::protobuf::RepeatedPtrField<Resource>* resources,
    ResourceFormat format)
{
  foreach (Resource& resource, *resources) {
    convertResourceFormat(&resource, format);
  }
}


void convertResourceFormat(
    std::vector<Resource>* resources,
    ResourceFormat format)
{
  foreach (Resource& resource, *resources) {
    convertResourceFormat(&resource, format);
  }
}


void convertResourceFormat(Offer::Operation* operation, ResourceFormat format)
{
  switch (operation->type()) {
    case Offer::Operation::LAUNCH: {
      Offer::Operation::Launch* launch = operation->mutable_launch();

      foreach (TaskInfo& task, *launch->mutable_task_infos()) {
        convertResourceFormat(task.mutable_resources(), format);

        if (task.has_executor()) {
          convertResourceFormat(
              task.mutable_executor()->mutable_resources(), format);
        }
      }
      break;
    }

    case Offer::Operation::LAUNCH_GROUP: {
      Offer::Operation::LaunchGroup* launchGroup =
        operation->mutable_launch_group();

      if (launchGroup->has_executor()) {
        convertResourceFormat(
            launchGroup->mutable_executor()->mutable_resources(), format);
      }

      TaskGroupInfo* taskGroup = launchGroup->mutable_task_group();

      foreach (TaskInfo& task, *taskGroup->mutable_tasks()) {
        convertResourceFormat(task.mutable_resources(), format);

        if (task.has_executor()) {
          convertResourceFormat(
              task.mutable_executor()->mutable_resources(), format);
        }
      }
      break;
    }

    case Offer::Operation::RESERVE: {
      convertResourceFormat(
          operation->mutable_reserve()->mutable_resources(), format);

      break;
    }

    case Offer::Operation::UNRESERVE: {
      convertResourceFormat(
          operation->mutable_unreserve()->mutable_resources(), format);

      break;
    }

    case Offer::Operation::CREATE: {
      convertResourceFormat(
          operation->mutable_create()->mutable_volumes(), format);

      break;
    }

    case Offer::Operation::DESTROY: {
      convertResourceFormat(
          operation->mutable_destroy()->mutable_volumes(), format);

      break;
    }

    case Offer::Operation::UNKNOWN:
      break; // No-op.
  }
}


Try<Nothing> downgradeResources(
    google::protobuf::RepeatedPtrField<Resource>* resources)
{
  foreach (const Resource& resource, *resources) {
    CHECK(!resource.has_role());
    CHECK(!resource.has_reservation());
  }

  foreach (const Resource& resource, *resources) {
    if (Resources::hasRefinedReservations(resource)) {
      return Error(
          "Invalid resources downgrade: resource " + stringify(resource) +
          " with refined reservations cannot be downgraded");
    }
  }

  convertResourceFormat(resources, PRE_RESERVATION_REFINEMENT);

  return Nothing();
}

} // namespace mesos {

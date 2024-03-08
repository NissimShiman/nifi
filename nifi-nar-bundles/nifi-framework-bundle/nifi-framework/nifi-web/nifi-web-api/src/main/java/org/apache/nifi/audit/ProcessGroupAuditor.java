/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.audit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;

import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audits process group creation/removal and configuration changes.
 */
@Aspect
public class ProcessGroupAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupAuditor.class);
 //   private FlowController flowController;

    /**
     * Audits the creation of process groups via createProcessGroup().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return group
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup createProcessGroup(String, org.apache.nifi.web.api.dto.ProcessGroupDTO))")
    public ProcessGroup createProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the process group
        ProcessGroup processGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the process group action...
        // audit process group creation
        final Action action = generateAuditRecord(processGroup, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return processGroup;
    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param processGroupDTO dto
     * @return group
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateProcessGroup(org.apache.nifi.web.api.dto.ProcessGroupDTO)) && "
            + "args(processGroupDTO)")
    public ProcessGroup updateProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, ProcessGroupDTO processGroupDTO) throws Throwable {
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());

        String name = processGroup.getName();
        ParameterContext parameterContext = processGroup.getParameterContext();
        String comments = processGroup.getComments();

        // perform the underlying operation
        ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<ActionDetails> details = new ArrayList<>();

            // see if the name has changed
            if (name != null && updatedProcessGroup.getName() != null && !name.equals(updatedProcessGroup.getName())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Name");
                configDetails.setValue(updatedProcessGroup.getName());
                configDetails.setPreviousValue(name);

                details.add(configDetails);
            }

            // see if the comments has changed
            if (comments != null && updatedProcessGroup.getComments() != null && !comments.equals(updatedProcessGroup.getComments())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Comments");
                configDetails.setValue(updatedProcessGroup.getComments());
                configDetails.setPreviousValue(comments);

                details.add(configDetails);
            }

            // see if the parameter context has changed
            if (parameterContext != null && updatedProcessGroup.getParameterContext() != null) {
                if (!parameterContext.getIdentifier().equals(updatedProcessGroup.getParameterContext().getIdentifier())) {
                    // create the config details
                    FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                    configDetails.setName("Parameter Context");
                    configDetails.setValue(updatedProcessGroup.getParameterContext().getIdentifier());
                    configDetails.setPreviousValue(parameterContext.getIdentifier());

                    details.add(configDetails);
                }
            } else if (updatedProcessGroup.getParameterContext() != null) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Parameter Context");
                configDetails.setValue(updatedProcessGroup.getParameterContext().getIdentifier());
                configDetails.setPreviousValue(null);

                details.add(configDetails);
            } else if (parameterContext != null) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Parameter Context");
                configDetails.setValue(null);
                configDetails.setPreviousValue(parameterContext.getIdentifier());

                details.add(configDetails);
            }

            // hold all actions
            Collection<Action> actions = new ArrayList<>();

            // save the actions if necessary
            if (!details.isEmpty()) {
                Date timestamp = new Date();

                // create the actions
                for (ActionDetails detail : details) {
                    // determine the type of operation being performed
                    Operation operation = Operation.Configure;
                    if (detail instanceof FlowChangeMoveDetails) {
                        operation = Operation.Move;
                    }

                    // create the port action for updating the name
                    FlowChangeAction processGroupAction = new FlowChangeAction();
                    processGroupAction.setUserIdentity(user.getIdentity());
                    processGroupAction.setOperation(operation);
                    processGroupAction.setTimestamp(timestamp);
                    processGroupAction.setSourceId(updatedProcessGroup.getIdentifier());
                    processGroupAction.setSourceName(updatedProcessGroup.getName());
                    processGroupAction.setSourceType(Component.ProcessGroup);
                    processGroupAction.setActionDetails(detail);

                    actions.add(processGroupAction);
                }
            }

            // save actions if necessary
            if (!actions.isEmpty()) {
                saveActions(actions, logger);
            }
        }

        return updatedProcessGroup;
    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state scheduled state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(void scheduleComponents(String, org.apache.nifi.controller.ScheduledState, java.util.Set<String>)) && "
        + "args(groupId, state, componentIds)")
    public void scheduleComponentsAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ScheduledState state, Set<String> componentIds) throws Throwable {
        logger.error("audit start/stop pg");
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        Set<ProcessorNode> processors = new HashSet<>();
   //     Map <ProcessorNode, ScheduledState> origProcessorsStates = new HashMap<>();
        for (ProcessorNode procNode: processGroup.getProcessors())
        {
            logger.error("B4: procNode, scheduledState: " + procNode.getName() + ", " + procNode.getScheduledState());
            if (!procNode.getScheduledState().equals(state) && !procNode.getScheduledState().equals(ScheduledState.DISABLED)) {
                processors.add(procNode);
                logger.error(procNode.getName() + "has been added");
            }

        }


        String name = processGroup.getName();
        List<ProcessGroup> processGroups =  processGroup.findAllProcessGroups();
        Set<String> processGroupIds = new HashSet<String>();
        for (ProcessGroup pg: processGroups) {
            String id = pg.getIdentifier();
            logger.error("new pg id: " + id);
            processGroupIds.add(id);
        }

        final Operation operation;

        proceedingJoinPoint.proceed();

        // determine the running state
        if (ScheduledState.RUNNING.equals(state)) {
            operation = Operation.Start;
        } else {
            operation = Operation.Stop;
        }

        saveUpdateAction(groupId, operation);
        // capture top level processors
        for (ProcessorNode procNode: processors)
        {
            logger.error("After: procNode, scheduledState: " + procNode.getName() + ", " + procNode.getScheduledState());
        //    if (procNode.getScheduledState().equals(state)) {
                saveProcessorUpdateAction(procNode, operation);
        //    }
        }

        for (String id: processGroupIds) {
            saveUpdateAction(id, operation);
        }

    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state scheduled state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(void enableComponents(String, org.apache.nifi.controller.ScheduledState, java.util.Set<String>)) && "
            + "args(groupId, state, componentIds)")
    public void enableComponentsAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ScheduledState state, Set<String> componentIds) throws Throwable {
 logger.error("audit enable pg");
        final Operation operation;

        proceedingJoinPoint.proceed();

        // determine the running state
        if (ScheduledState.DISABLED.equals(state)) {
            operation = Operation.Disable;
        } else {
            operation = Operation.Enable;
        }

        saveUpdateAction(groupId, operation);
    }

    /**
     * Audits the update of controller serivce state
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state controller service state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(void activateControllerServices(String, org.apache.nifi.controller.service.ControllerServiceState, java.util.Collection<String>)) && "
        + "args(groupId, state, serviceIds)")
    public void activateControllerServicesAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ControllerServiceState state, Collection<String> serviceIds) throws Throwable {
        final Operation operation;

        List<ControllerServiceNode> controllerServiceNodes = getServicesChangingState(groupId, state, serviceIds);

        proceedingJoinPoint.proceed();

        // determine the service state
        if (ControllerServiceState.ENABLED.equals(state)) {
            operation = Operation.Enable;
        } else {
            operation = Operation.Disable;
        }

        saveUpdateAction(groupId, operation);
        for (ControllerServiceNode csNode : controllerServiceNodes) {
    //        if (ControllerServiceState.ENABLED.equals(state) && isControllerServiceEnabled(csNode) ||
      //              ControllerServiceState.DISABLED.equals(state) && isControllerServiceDisabled(csNode)) {
                saveUpdateControllerServiceAction(csNode, operation);
        //    }
        }
  //      saveUpdateControllerServiceAction(groupId, controllerServiceStates.keySet() , operation);
    }

    /**
     * Returns whether the specified controller service is disabled (or disabling).
     *
     * @param controllerService service
     * @return whether the specified controller service is disabled (or disabling)
     */
    private boolean isControllerServiceDisabled(final ControllerServiceNode controllerService) {
        return ControllerServiceState.DISABLED.equals(controllerService.getState()) || ControllerServiceState.DISABLING.equals(controllerService.getState());
    }

    /**
     * Returns whether the specified controller service is enabled (or enabling).
     *
     * @param controllerService service
     * @return whether the specified controller service is enabled (or enabling)
     */
    private boolean isControllerServiceEnabled(final ControllerServiceNode controllerService) {
        return ControllerServiceState.ENABLED.equals(controllerService.getState()) || ControllerServiceState.ENABLING.equals(controllerService.getState());
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateProcessGroupFlow(..))")
    public ProcessGroup updateProcessGroupFlowAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final Object[] args = proceedingJoinPoint.getArgs();
        final String groupId = (String) args[0];

        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final VersionControlInformation vci = processGroup.getVersionControlInformation();

        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();
        final VersionControlInformation updatedVci = updatedProcessGroup.getVersionControlInformation();

        final Operation operation;
        if (vci == null) {
            operation = Operation.StartVersionControl;
        } else {
            if (updatedVci == null) {
                operation = Operation.StopVersionControl;
            } else if (vci.getVersion() == updatedVci.getVersion()) {
                operation = Operation.RevertLocalChanges;
            } else {
                operation = Operation.ChangeVersion;
            }
        }

        saveUpdateAction(groupId, operation);

        return updatedProcessGroup;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateVersionControlInformation(..))")
    public ProcessGroup updateVersionControlInformationAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final VersionControlInformationDTO vciDto = (VersionControlInformationDTO) proceedingJoinPoint.getArgs()[0];

        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(vciDto.getGroupId());
        final VersionControlInformation vci = processGroup.getVersionControlInformation();

        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        final Operation operation;
        if (vci == null) {
            operation = Operation.StartVersionControl;
        } else {
            operation = Operation.CommitLocalChanges;
        }

        saveUpdateAction(vciDto.getGroupId(), operation);

        return updatedProcessGroup;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup disconnectVersionControl(String)) && "
            + "args(groupId)")
    public ProcessGroup disconnectVersionControlAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String groupId) throws Throwable {
        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        saveUpdateAction(groupId, Operation.StopVersionControl);

        return updatedProcessGroup;
    }

    private void saveUpdateAction(final String groupId, final Operation operation) throws Throwable {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // if the user was starting/stopping this process group
        FlowChangeAction action = new FlowChangeAction();
        action.setUserIdentity(user.getIdentity());
        action.setSourceId(processGroup.getIdentifier());
        action.setSourceName(processGroup.getName());
        action.setSourceType(Component.ProcessGroup);
        action.setTimestamp(new Date());
        action.setOperation(operation);

        // add this action
        saveAction(action, logger);
    }

    private void saveProcessorUpdateAction(ProcessorNode procNode, final Operation operation) {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
//        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
//        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // if the user was starting/stopping this process group
        FlowChangeAction action = new FlowChangeAction();
        action.setUserIdentity(user.getIdentity());
        action.setSourceId(procNode.getIdentifier());
        action.setSourceName(procNode.getName());
        action.setSourceType(Component.Processor);
        action.setTimestamp(new Date());
        action.setOperation(operation);

        // add this action
        saveAction(action, logger);

    }

    private void saveUpdateControllerServiceAction(ControllerServiceNode csNode, final Operation operation) throws Throwable {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
//        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
  //      ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
//        for (ControllerServiceNode csNode : serviceNodes) {
       //     logger.error("cs id audited: " + serviceId.getName());
 //           ControllerServiceNode csNode =     processGroup.getControllerService(serviceId, true, true);
   //         if (csNode != null) {
logger.error("csNode5 audited: " + csNode.getName());
                // if the user was enabling/disabling this controller Service
                FlowChangeAction action = new FlowChangeAction();
                action.setUserIdentity(user.getIdentity());
                action.setSourceId(csNode.getIdentifier());
                action.setSourceName(csNode.getName());
                action.setSourceType(Component.ControllerService);
                action.setTimestamp(new Date());
                action.setOperation(operation);

                FlowChangeExtensionDetails serviceDetails = new FlowChangeExtensionDetails();
                serviceDetails.setType(csNode.getComponentType());
                action.setComponentDetails(serviceDetails);

                // add this action
                saveAction(action, logger);
     //       }
  //      }
    }
//    Map<ControllerServiceNode, ControllerServiceState> controllerServiceStates = determineServicesThatCouldChangeState(String groupId, ControllerServiceState state, Collection<String> serviceIds, final Operation operation);

    private List<ControllerServiceNode> getServicesChangingState(final String groupId, final ControllerServiceState state, Collection<String> serviceIds) throws Throwable {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        List<ControllerServiceNode> csNodes = new ArrayList<>();
        for (String serviceId : serviceIds) {
            logger.error("cs pre audited -1: " + serviceId);
            ControllerServiceNode csNode =     processGroup.findControllerService(serviceId, true, true);
            if (csNode != null) {
logger.error("csNode pre- audited 2: " + csNode.getName());
                if ((isControllerServiceDisabled(csNode) && state.equals(ControllerServiceState.ENABLED)) || isControllerServiceEnabled(csNode) && state.equals(ControllerServiceState.DISABLED)) {
                    csNodes.add(csNode);
                }
            }
        }
        return csNodes;
    }


    /**
     * Audits the removal of a process group via deleteProcessGroup().
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(void deleteProcessGroup(String)) && "
            + "args(groupId)")
    public void removeProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId) throws Throwable {
        // get the process group before removing it
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // remove the process group
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the process group removal
        final Action action = generateAuditRecord(processGroup, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a process group.
     *
     * @param processGroup group
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation) {
        return generateAuditRecord(processGroup, operation, null);
    }

    /**
     * Generates an audit record for the creation of a process group.
     *
     * @param processGroup group
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {

            // create the process group action for adding this process group
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(processGroup.getIdentifier());
            action.setSourceName(processGroup.getName());
            action.setSourceType(Component.ProcessGroup);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}

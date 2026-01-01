package executors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/flyteorg/flyte/flytepropeller/pkg/apis/flyteworkflow/v1alpha1/mocks"
)

type immExecContext struct {
	ImmutableExecutionContext
}

type tdGetter struct {
	TaskDetailsGetter
}

type subWfGetter struct {
	SubWorkflowGetter
}

type immutableParentInfo struct {
	ImmutableParentInfo
}

func TestExecutionContext(t *testing.T) {
	eCtx := immExecContext{}
	taskGetter := tdGetter{}
	subWfGetter := subWfGetter{}
	immutableParentInfo := immutableParentInfo{}

	ec := NewExecutionContext(eCtx, taskGetter, subWfGetter, immutableParentInfo, InitializeControlFlow())
	assert.NotNil(t, ec)
	typed := ec.(execContext)
	assert.Equal(t, typed.ImmutableExecutionContext, eCtx)
	assert.Equal(t, typed.SubWorkflowGetter, subWfGetter)
	assert.Equal(t, typed.TaskDetailsGetter, taskGetter)
	assert.Equal(t, typed.GetParentInfo(), immutableParentInfo)

	taskGetter2 := tdGetter{}
	NewExecutionContextWithTasksGetter(ec, taskGetter2)
	assert.NotNil(t, ec)
	typed = ec.(execContext)
	assert.Equal(t, typed.ImmutableExecutionContext, eCtx)
	assert.Equal(t, typed.SubWorkflowGetter, subWfGetter)
	assert.Equal(t, typed.TaskDetailsGetter, taskGetter2)
	assert.Equal(t, typed.GetParentInfo(), immutableParentInfo)

	subWfGetter2 := subWfGetter
	NewExecutionContextWithWorkflowGetter(ec, subWfGetter2)
	assert.NotNil(t, ec)
	typed = ec.(execContext)
	assert.Equal(t, typed.ImmutableExecutionContext, eCtx)
	assert.Equal(t, typed.SubWorkflowGetter, subWfGetter2)
	assert.Equal(t, typed.TaskDetailsGetter, taskGetter)
	assert.Equal(t, typed.GetParentInfo(), immutableParentInfo)

	immutableParentInfo2 := immutableParentInfo
	NewExecutionContextWithParentInfo(ec, immutableParentInfo2)
	assert.NotNil(t, ec)
	typed = ec.(execContext)
	assert.Equal(t, typed.ImmutableExecutionContext, eCtx)
	assert.Equal(t, typed.SubWorkflowGetter, subWfGetter2)
	assert.Equal(t, typed.TaskDetailsGetter, taskGetter)
	assert.Equal(t, typed.GetParentInfo(), immutableParentInfo2)
}

func TestParentExecutionInfo_GetUniqueID(t *testing.T) {
	expectedID := "testID"
	parentInfo := NewParentInfo(expectedID, 1, false)
	assert.Equal(t, expectedID, parentInfo.GetUniqueID())
}

func TestParentExecutionInfo_CurrentAttempt(t *testing.T) {
	expectedAttempt := uint32(123465)
	parentInfo := NewParentInfo("testID", expectedAttempt, false)
	assert.Equal(t, expectedAttempt, parentInfo.CurrentAttempt())
}

func TestParentExecutionInfo_DynamicChain(t *testing.T) {
	expectedAttempt := uint32(123465)
	parentInfo := NewParentInfo("testID", expectedAttempt, true)
	assert.True(t, parentInfo.IsInDynamicChain())
}

func TestControlFlow_ControlFlowParallelism(t *testing.T) {
	cFlow := InitializeControlFlow().(*controlFlow)
	assert.Equal(t, uint32(0), cFlow.CurrentParallelism())
	cFlow.IncrementParallelism()
	assert.Equal(t, uint32(1), cFlow.CurrentParallelism())
	cFlow.IncrementParallelism()
	assert.Equal(t, uint32(2), cFlow.CurrentParallelism())
}

func TestNewParentInfo(t *testing.T) {
	expectedID := "testID"
	expectedAttempt := uint32(123465)
	parentInfo := NewParentInfo(expectedID, expectedAttempt, false).(*parentExecutionInfo)
	assert.Equal(t, expectedID, parentInfo.uniqueID)
	assert.Equal(t, expectedAttempt, parentInfo.currentAttempts)
}

func TestTaskDetailsGetter_GetTask(t *testing.T) {
	t.Run("returns task successfully", func(t *testing.T) {
		mockTask := mocks.NewExecutableTask(t)
		mockTask.EXPECT().TaskType().Return("python-task").Maybe()

		taskGetter := mocks.NewTaskDetailsGetter(t)
		taskGetter.EXPECT().GetTask("task-1").Return(mockTask, nil)

		task, err := taskGetter.GetTask("task-1")
		assert.NoError(t, err)
		assert.NotNil(t, task)
		assert.Equal(t, "python-task", task.TaskType())
	})

	t.Run("returns error when task not found", func(t *testing.T) {
		taskGetter := mocks.NewTaskDetailsGetter(t)
		taskGetter.EXPECT().GetTask("non-existent-task").Return(nil, errors.New("task not found"))

		task, err := taskGetter.GetTask("non-existent-task")
		assert.Error(t, err)
		assert.Nil(t, task)
		assert.Contains(t, err.Error(), "task not found")
	})

	t.Run("handles empty task id", func(t *testing.T) {
		taskGetter := mocks.NewTaskDetailsGetter(t)
		taskGetter.EXPECT().GetTask("").Return(nil, errors.New("invalid task id"))

		task, err := taskGetter.GetTask("")
		assert.Error(t, err)
		assert.Nil(t, task)
	})
}

func TestExecutionContext_GetTask(t *testing.T) {
	t.Run("execution context delegates to task getter", func(t *testing.T) {
		mockTask := mocks.NewExecutableTask(t)
		mockTask.EXPECT().TaskType().Return("container-task").Maybe()

		taskGetter := mocks.NewTaskDetailsGetter(t)
		taskGetter.EXPECT().GetTask("my-task").Return(mockTask, nil)

		eCtx := immExecContext{}
		subWfGetter := subWfGetter{}
		immutableParentInfo := immutableParentInfo{}

		ec := NewExecutionContext(eCtx, taskGetter, subWfGetter, immutableParentInfo, InitializeControlFlow())
		assert.NotNil(t, ec)

		task, err := ec.GetTask("my-task")
		assert.NoError(t, err)
		assert.NotNil(t, task)
		assert.Equal(t, "container-task", task.TaskType())
	})

	t.Run("execution context returns error from task getter", func(t *testing.T) {
		taskGetter := mocks.NewTaskDetailsGetter(t)
		taskGetter.EXPECT().GetTask("missing-task").Return(nil, errors.New("task does not exist"))

		eCtx := immExecContext{}
		subWfGetter := subWfGetter{}
		immutableParentInfo := immutableParentInfo{}

		ec := NewExecutionContext(eCtx, taskGetter, subWfGetter, immutableParentInfo, InitializeControlFlow())
		assert.NotNil(t, ec)

		task, err := ec.GetTask("missing-task")
		assert.Error(t, err)
		assert.Nil(t, task)
		assert.Contains(t, err.Error(), "task does not exist")
	})
}

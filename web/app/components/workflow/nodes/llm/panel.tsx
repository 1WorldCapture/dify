import type { FC } from 'react'
import React, { useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import MemoryConfig from '../_base/components/memory-config'
import VarReferencePicker from '../_base/components/variable/var-reference-picker'
import useConfig from './use-config'
import ResolutionPicker from './components/resolution-picker'
import type { LLMNodeType } from './types'
import ConfigPrompt from './components/config-prompt'
import VarList from '@/app/components/workflow/nodes/_base/components/variable/var-list'
import Field from '@/app/components/workflow/nodes/_base/components/field'
import AddButton from '@/app/components/base/button/add-button'
import Split from '@/app/components/workflow/nodes/_base/components/split'
import ModelParameterModal from '@/app/components/header/account-setting/model-provider-page/model-parameter-modal'
import OutputVars, { VarItem } from '@/app/components/workflow/nodes/_base/components/output-vars'
import { Resolution } from '@/types/app'
import { InputVarType, type NodePanelProps } from '@/app/components/workflow/types'
import BeforeRunForm from '@/app/components/workflow/nodes/_base/components/before-run-form'
import type { Props as FormProps } from '@/app/components/workflow/nodes/_base/components/before-run-form/form'
import ResultPanel from '@/app/components/workflow/run/result-panel'
import { useModelListAndDefaultModelAndCurrentProviderAndModel } from '@/app/components/header/account-setting/model-provider-page/hooks'

const i18nPrefix = 'workflow.nodes.llm'

const Panel: FC<NodePanelProps<LLMNodeType>> = ({
  id,
  data,
}) => {
  const { t } = useTranslation()
  const readOnly = false
  const {
    currentProvider,
    currentModel,
  } = useModelListAndDefaultModelAndCurrentProviderAndModel(1)

  const {
    inputs,
    isChatModel,
    isCompletionModel,
    isShowVisionConfig,
    handleModelChanged,
    handleCompletionParamsChange,
    handleVarListChange,
    handleAddVariable,
    handleContextVarChange,
    handlePromptChange,
    handleMemoryChange,
    handleVisionResolutionChange,
    isShowSingleRun,
    hideSingleRun,
    inputVarValues,
    setInputVarValues,
    visionFiles,
    setVisionFiles,
    contexts,
    setContexts,
    runningStatus,
    handleRun,
    handleStop,
    varInputs,
    runResult,
  } = useConfig(id, data)

  const isChatApp = true // TODO: get from app context
  const model = inputs.model

  const singleRunForms = (() => {
    const forms: FormProps[] = []

    if (varInputs.length > 0) {
      forms.push(
        {
          label: t(`${i18nPrefix}.singleRun.variable`)!,
          inputs: varInputs,
          values: inputVarValues,
          onChange: setInputVarValues,
        },
      )
    }

    if (inputs.context?.variable_selector && inputs.context?.variable_selector.length > 0) {
      forms.push(
        {
          label: t(`${i18nPrefix}.context`)!,
          inputs: [{
            label: '',
            variable: 'contexts',
            type: InputVarType.contexts,
            required: false,
          }],
          values: { contexts },
          onChange: keyValue => setContexts((keyValue as any).contexts),
        },
      )
    }

    if (isShowVisionConfig) {
      forms.push(
        {
          label: t(`${i18nPrefix}.vision`)!,
          inputs: [{
            label: t(`${i18nPrefix}.files`)!,
            variable: 'visionFiles',
            type: InputVarType.files,
            required: false,
          }],
          values: { visionFiles },
          onChange: keyValue => setVisionFiles((keyValue as any).visionFiles),
        },
      )
    }

    return forms
  })()

  useEffect(() => {
    if (currentProvider?.provider && currentModel?.model && !model.provider) {
      handleModelChanged({
        provider: currentProvider?.provider,
        modelId: currentModel?.model,
        mode: currentModel?.model_properties?.mode as string,
      })
    }
  }, [model.provider, currentProvider, currentModel, handleModelChanged])

  return (
    <div className='mt-2'>
      <div className='px-4 pb-4 space-y-4'>
        <Field
          title={t(`${i18nPrefix}.model`)}
        >
          <ModelParameterModal
            popupClassName='!w-[387px]'
            isAdvancedMode={true}
            mode={model?.mode}
            provider={model?.provider}
            completionParams={model?.completion_params}
            modelId={model?.name}
            setModel={handleModelChanged}
            onCompletionParamsChange={handleCompletionParamsChange}
            hideDebugWithMultipleModel
            debugWithMultipleModel={false}
          />
        </Field>

        <Field
          title={t(`${i18nPrefix}.variables`)}
          operations={
            <AddButton onClick={handleAddVariable} />
          }
        >
          <VarList
            readonly={readOnly}
            nodeId={id}
            list={inputs.variables}
            onChange={handleVarListChange}
          />
        </Field>

        {/* knowledge */}
        <Field
          title={t(`${i18nPrefix}.context`)}
          tooltip={t(`${i18nPrefix}.contextTooltip`)!}
        >
          <VarReferencePicker
            readonly={readOnly}
            nodeId={id}
            isShowNodeName
            value={inputs.context?.variable_selector || []}
            onChange={handleContextVarChange}
          />

        </Field>

        {/* Prompt */}
        {model.name && (
          <ConfigPrompt
            readOnly={readOnly}
            isChatModel={isChatModel}
            payload={inputs.prompt_template}
            variables={inputs.variables.map(item => item.variable)}
            onChange={handlePromptChange}
          />
        )}

        {/* Memory examples. Wait for design */}
        {/* {isChatApp && isChatModel && (
          <div className='text-xs text-gray-300'>Memory examples(Designing)</div>
        )} */}
        {/* Memory */}
        {isChatApp && (
          <>
            <MemoryConfig
              readonly={readOnly}
              payload={inputs.memory}
              onChange={handleMemoryChange}
              canSetRoleName={isCompletionModel}
            />
            <Split />
          </>
        )}

        {/* Vision: GPT4-vision and so on */}
        {isShowVisionConfig && (
          <Field
            title={t(`${i18nPrefix}.vision`)}
            tooltip={t('appDebug.vision.description')!}
            operations={
              <ResolutionPicker
                value={inputs.vision.configs?.detail || Resolution.high}
                onChange={handleVisionResolutionChange}
              />
            }
          />
        )}
      </div>
      <div className='px-4 pt-4 pb-2'>
        <OutputVars>
          <>
            <VarItem
              name='output'
              type='string'
              description={t(`${i18nPrefix}.outputVars.output`)}
            />
            <VarItem
              name='usage'
              type='object'
              description={t(`${i18nPrefix}.outputVars.usage`)}
            />
          </>
        </OutputVars>
      </div>
      {isShowSingleRun && (
        <BeforeRunForm
          nodeName={inputs.title}
          onHide={hideSingleRun}
          forms={singleRunForms}
          runningStatus={runningStatus}
          onRun={handleRun}
          onStop={handleStop}
          result={<ResultPanel {...runResult} showSteps={false} />}
        />
      )}
    </div>
  )
}

export default React.memo(Panel)
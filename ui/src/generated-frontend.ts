import { gql } from '@apollo/client'
import * as Apollo from '@apollo/client'
export type Maybe<T> = T | null
export type Exact<T extends { [key: string]: unknown }> = {
  [K in keyof T]: T[K]
}
export type MakeOptional<T, K extends keyof T> = Omit<T, K> &
  { [SubKey in K]?: Maybe<T[SubKey]> }
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> &
  { [SubKey in K]: Maybe<T[SubKey]> }
const defaultOptions = {}
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string
  String: string
  Boolean: boolean
  Int: number
  Float: number
  /** DateTime */
  DateTimeUtc: any
}

export type AddOrderInput = {
  exchg: Scalars['String']
  orderType: OrderTypeInput
  side: TradeTypeInput
  pair: Scalars['String']
  quantity: Scalars['Float']
  price: Scalars['Float']
  /** Set this to true to pass a real order */
  dryRun: Scalars['Boolean']
}

export type Model = {
  __typename?: 'Model'
  id: Scalars['String']
  json: Scalars['String']
}

export type ModelReset = {
  /** The model name, if unspecified all models are reset */
  name?: Maybe<Scalars['String']>
  /** whether to stop trading during this operation */
  stopTrading: Scalars['Boolean']
  /** whether to restart afterwards */
  restartAfter: Scalars['Boolean']
}

export enum MutableField {
  ValueStrat = 'value_strat',
  Pnl = 'pnl',
  NominalPosition = 'nominal_position',
  PreviousValueStrat = 'previous_value_strat',
}

export type MutationRoot = {
  __typename?: 'MutationRoot'
  /** Get all positions for this strat */
  state: Scalars['Boolean']
  /** Cancel the ongoing operation */
  cancelOngoingOp: Scalars['Boolean']
  /** Reset the specified model */
  resetModel: StrategyStatus
  /** Send a lifecycle command to the strategy */
  lifecycleCmd: StrategyStatus
  /** Add an order (for testing) */
  addOrder: OrderResult
  /** Pass an order with an order manager */
  passOrder: Scalars['String']
}

export type MutationRootStateArgs = {
  tk: TypeAndKeyInput
  fm: StateFieldMutation
}

export type MutationRootCancelOngoingOpArgs = {
  tk: TypeAndKeyInput
}

export type MutationRootResetModelArgs = {
  tk: TypeAndKeyInput
  mr: ModelReset
}

export type MutationRootLifecycleCmdArgs = {
  tk: TypeAndKeyInput
  slc: StrategyLifecycleCmd
}

export type MutationRootAddOrderArgs = {
  input: AddOrderInput
}

export type MutationRootPassOrderArgs = {
  exchange: Scalars['String']
  input: AddOrderInput
}

export type OperationHistory = {
  __typename?: 'OperationHistory'
  id: Scalars['String']
  kind: OperationKind
  transactions: Array<TransactionHistory>
  ts: Scalars['DateTimeUtc']
}

export enum OperationKind {
  Open = 'OPEN',
  Close = 'CLOSE',
}

export enum OrderMode {
  Market = 'MARKET',
  Limit = 'LIMIT',
}

export type OrderResult = {
  __typename?: 'OrderResult'
  identifier: Scalars['String']
}

export enum OrderTypeInput {
  Limit = 'LIMIT',
  Market = 'MARKET',
}

export enum PositionKind {
  Short = 'SHORT',
  Long = 'LONG',
}

export type QueryRoot = {
  __typename?: 'QueryRoot'
  /** List of all strats */
  strats: Array<StrategyState>
  /** Dump the current in memory state for a strategy */
  stratState: Scalars['String']
  /** Current strategy status */
  stratStatus: Scalars['String']
  /** Get all operations for this strat */
  operations: Array<OperationHistory>
  /** Get the ongoing operation for the strat */
  currentOperation?: Maybe<OperationHistory>
  /** Get all transactions history for an order manager */
  transactions: Array<Scalars['String']>
  /** Get all transactions of a single order */
  orderTransactions: Array<Scalars['String']>
  /** Get the latest model values */
  models: Array<Model>
}

export type QueryRootStratStateArgs = {
  tk: TypeAndKeyInput
}

export type QueryRootStratStatusArgs = {
  tk: TypeAndKeyInput
}

export type QueryRootOperationsArgs = {
  tk: TypeAndKeyInput
}

export type QueryRootCurrentOperationArgs = {
  tk: TypeAndKeyInput
}

export type QueryRootTransactionsArgs = {
  exchange: Scalars['String']
}

export type QueryRootOrderTransactionsArgs = {
  exchange: Scalars['String']
  orderId: Scalars['String']
}

export type QueryRootModelsArgs = {
  tk: TypeAndKeyInput
}

export type StateFieldMutation = {
  field: MutableField
  value: Scalars['Float']
}

export enum StrategyLifecycleCmd {
  Restart = 'RESTART',
  StopTrading = 'STOP_TRADING',
  ResumeTrading = 'RESUME_TRADING',
}

export type StrategyState = {
  __typename?: 'StrategyState'
  type: Scalars['String']
  id: Scalars['String']
  state: Scalars['String']
  status: Scalars['String']
}

export enum StrategyStatus {
  Stopped = 'STOPPED',
  Running = 'RUNNING',
  NotTrading = 'NOT_TRADING',
}

export type Subscription = {
  __typename?: 'Subscription'
  helloWorld: Scalars['String']
}

export enum TradeKind {
  Buy = 'BUY',
  Sell = 'SELL',
}

export type TradeOperation = {
  __typename?: 'TradeOperation'
  kind: TradeKind
  pair: Scalars['String']
  qty: Scalars['Float']
  price: Scalars['Float']
  dryMode: Scalars['Boolean']
  mode: OrderMode
  assetType: Scalars['String']
}

export enum TradeTypeInput {
  Sell = 'SELL',
  Buy = 'BUY',
}

export type TransactionHistory = {
  __typename?: 'TransactionHistory'
  value: Scalars['Float']
  pair: Scalars['String']
  time: Scalars['DateTimeUtc']
  pos: PositionKind
  price: Scalars['Float']
  lastTransaction?: Maybe<Scalars['String']>
  trade: TradeOperation
  lastOrder?: Maybe<Scalars['String']>
  qty: Scalars['Float']
}

export type TypeAndKeyInput = {
  type: Scalars['String']
  id: Scalars['String']
}

export type ModelFieldsFragment = {
  __typename?: 'Model'
  id: string
  json: string
}

export type ModelsQueryVariables = Exact<{
  tk: TypeAndKeyInput
}>

export type ModelsQuery = {
  __typename?: 'QueryRoot'
  models: Array<{ __typename?: 'Model'; id: string; json: string }>
}

export type OpHistFieldsFragment = {
  __typename?: 'OperationHistory'
  kind: OperationKind
  id: string
  transactions: Array<{
    __typename?: 'TransactionHistory'
    value: number
    pair: string
    time: any
    pos: PositionKind
    price: number
    lastOrder?: Maybe<string>
    qty: number
    trade: { __typename?: 'TradeOperation'; kind: TradeKind }
  }>
}

export type OperationsQueryVariables = Exact<{
  tk: TypeAndKeyInput
}>

export type OperationsQuery = {
  __typename?: 'QueryRoot'
  operations: Array<{
    __typename?: 'OperationHistory'
    kind: OperationKind
    id: string
    transactions: Array<{
      __typename?: 'TransactionHistory'
      value: number
      pair: string
      time: any
      pos: PositionKind
      price: number
      lastOrder?: Maybe<string>
      qty: number
      trade: { __typename?: 'TradeOperation'; kind: TradeKind }
    }>
  }>
}

export type OrderTransactionsQueryVariables = Exact<{
  xch: Scalars['String']
  id: Scalars['String']
}>

export type OrderTransactionsQuery = {
  __typename?: 'QueryRoot'
  transactions: Array<string>
}

export type StratsQueryVariables = Exact<{ [key: string]: never }>

export type StratsQuery = {
  __typename?: 'QueryRoot'
  strats: Array<{ __typename?: 'StrategyState'; type: string; id: string }>
}

export type StratsExtQueryVariables = Exact<{ [key: string]: never }>

export type StratsExtQuery = {
  __typename?: 'QueryRoot'
  strats: Array<{
    __typename?: 'StrategyState'
    type: string
    id: string
    state: string
    status: string
  }>
}

export type ChangeStratStateMutationVariables = Exact<{
  tktc: TypeAndKeyInput
  fm: StateFieldMutation
}>

export type ChangeStratStateMutation = {
  __typename?: 'MutationRoot'
  state: boolean
}

export type CancelMutationVariables = Exact<{
  tktc: TypeAndKeyInput
}>

export type CancelMutation = {
  __typename?: 'MutationRoot'
  cancelOngoingOp: boolean
}

export type ResetModelMutationVariables = Exact<{
  tktc: TypeAndKeyInput
  mr: ModelReset
}>

export type ResetModelMutation = {
  __typename?: 'MutationRoot'
  resetModel: StrategyStatus
}

export type LifecycleCmdMutationVariables = Exact<{
  tktc: TypeAndKeyInput
  lc: StrategyLifecycleCmd
}>

export type LifecycleCmdMutation = {
  __typename?: 'MutationRoot'
  lifecycleCmd: StrategyStatus
}

export const ModelFieldsFragmentDoc = gql`
  fragment modelFields on Model {
    id
    json
  }
`
export const OpHistFieldsFragmentDoc = gql`
  fragment opHistFields on OperationHistory {
    kind
    id
    transactions {
      value
      pair
      time
      pos
      price
      lastOrder
      trade {
        kind
      }
      qty
    }
  }
`
export const ModelsDocument = gql`
  query models($tk: TypeAndKeyInput!) {
    models: models(tk: $tk) {
      ...modelFields
    }
  }
  ${ModelFieldsFragmentDoc}
`

/**
 * __useModelsQuery__
 *
 * To run a query within a React component, call `useModelsQuery` and pass it any options that fit your needs.
 * When your component renders, `useModelsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useModelsQuery({
 *   variables: {
 *      tk: // value for 'tk'
 *   },
 * });
 */
export function useModelsQuery(
  baseOptions: Apollo.QueryHookOptions<ModelsQuery, ModelsQueryVariables>
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useQuery<ModelsQuery, ModelsQueryVariables>(
    ModelsDocument,
    options
  )
}
export function useModelsLazyQuery(
  baseOptions?: Apollo.LazyQueryHookOptions<ModelsQuery, ModelsQueryVariables>
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useLazyQuery<ModelsQuery, ModelsQueryVariables>(
    ModelsDocument,
    options
  )
}
export type ModelsQueryHookResult = ReturnType<typeof useModelsQuery>
export type ModelsLazyQueryHookResult = ReturnType<typeof useModelsLazyQuery>
export type ModelsQueryResult = Apollo.QueryResult<
  ModelsQuery,
  ModelsQueryVariables
>
export function refetchModelsQuery(variables?: ModelsQueryVariables) {
  return { query: ModelsDocument, variables: variables }
}
export const OperationsDocument = gql`
  query operations($tk: TypeAndKeyInput!) {
    operations: operations(tk: $tk) {
      ...opHistFields
    }
  }
  ${OpHistFieldsFragmentDoc}
`

/**
 * __useOperationsQuery__
 *
 * To run a query within a React component, call `useOperationsQuery` and pass it any options that fit your needs.
 * When your component renders, `useOperationsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useOperationsQuery({
 *   variables: {
 *      tk: // value for 'tk'
 *   },
 * });
 */
export function useOperationsQuery(
  baseOptions: Apollo.QueryHookOptions<
    OperationsQuery,
    OperationsQueryVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useQuery<OperationsQuery, OperationsQueryVariables>(
    OperationsDocument,
    options
  )
}
export function useOperationsLazyQuery(
  baseOptions?: Apollo.LazyQueryHookOptions<
    OperationsQuery,
    OperationsQueryVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useLazyQuery<OperationsQuery, OperationsQueryVariables>(
    OperationsDocument,
    options
  )
}
export type OperationsQueryHookResult = ReturnType<typeof useOperationsQuery>
export type OperationsLazyQueryHookResult = ReturnType<
  typeof useOperationsLazyQuery
>
export type OperationsQueryResult = Apollo.QueryResult<
  OperationsQuery,
  OperationsQueryVariables
>
export function refetchOperationsQuery(variables?: OperationsQueryVariables) {
  return { query: OperationsDocument, variables: variables }
}
export const OrderTransactionsDocument = gql`
  query orderTransactions($xch: String!, $id: String!) {
    transactions: orderTransactions(exchange: $xch, orderId: $id)
  }
`

/**
 * __useOrderTransactionsQuery__
 *
 * To run a query within a React component, call `useOrderTransactionsQuery` and pass it any options that fit your needs.
 * When your component renders, `useOrderTransactionsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useOrderTransactionsQuery({
 *   variables: {
 *      xch: // value for 'xch'
 *      id: // value for 'id'
 *   },
 * });
 */
export function useOrderTransactionsQuery(
  baseOptions: Apollo.QueryHookOptions<
    OrderTransactionsQuery,
    OrderTransactionsQueryVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useQuery<
    OrderTransactionsQuery,
    OrderTransactionsQueryVariables
  >(OrderTransactionsDocument, options)
}
export function useOrderTransactionsLazyQuery(
  baseOptions?: Apollo.LazyQueryHookOptions<
    OrderTransactionsQuery,
    OrderTransactionsQueryVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useLazyQuery<
    OrderTransactionsQuery,
    OrderTransactionsQueryVariables
  >(OrderTransactionsDocument, options)
}
export type OrderTransactionsQueryHookResult = ReturnType<
  typeof useOrderTransactionsQuery
>
export type OrderTransactionsLazyQueryHookResult = ReturnType<
  typeof useOrderTransactionsLazyQuery
>
export type OrderTransactionsQueryResult = Apollo.QueryResult<
  OrderTransactionsQuery,
  OrderTransactionsQueryVariables
>
export function refetchOrderTransactionsQuery(
  variables?: OrderTransactionsQueryVariables
) {
  return { query: OrderTransactionsDocument, variables: variables }
}
export const StratsDocument = gql`
  query strats {
    strats {
      type
      id
    }
  }
`

/**
 * __useStratsQuery__
 *
 * To run a query within a React component, call `useStratsQuery` and pass it any options that fit your needs.
 * When your component renders, `useStratsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useStratsQuery({
 *   variables: {
 *   },
 * });
 */
export function useStratsQuery(
  baseOptions?: Apollo.QueryHookOptions<StratsQuery, StratsQueryVariables>
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useQuery<StratsQuery, StratsQueryVariables>(
    StratsDocument,
    options
  )
}
export function useStratsLazyQuery(
  baseOptions?: Apollo.LazyQueryHookOptions<StratsQuery, StratsQueryVariables>
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useLazyQuery<StratsQuery, StratsQueryVariables>(
    StratsDocument,
    options
  )
}
export type StratsQueryHookResult = ReturnType<typeof useStratsQuery>
export type StratsLazyQueryHookResult = ReturnType<typeof useStratsLazyQuery>
export type StratsQueryResult = Apollo.QueryResult<
  StratsQuery,
  StratsQueryVariables
>
export function refetchStratsQuery(variables?: StratsQueryVariables) {
  return { query: StratsDocument, variables: variables }
}
export const StratsExtDocument = gql`
  query stratsExt {
    strats {
      type
      id
      state
      status
    }
  }
`

/**
 * __useStratsExtQuery__
 *
 * To run a query within a React component, call `useStratsExtQuery` and pass it any options that fit your needs.
 * When your component renders, `useStratsExtQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useStratsExtQuery({
 *   variables: {
 *   },
 * });
 */
export function useStratsExtQuery(
  baseOptions?: Apollo.QueryHookOptions<StratsExtQuery, StratsExtQueryVariables>
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useQuery<StratsExtQuery, StratsExtQueryVariables>(
    StratsExtDocument,
    options
  )
}
export function useStratsExtLazyQuery(
  baseOptions?: Apollo.LazyQueryHookOptions<
    StratsExtQuery,
    StratsExtQueryVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useLazyQuery<StratsExtQuery, StratsExtQueryVariables>(
    StratsExtDocument,
    options
  )
}
export type StratsExtQueryHookResult = ReturnType<typeof useStratsExtQuery>
export type StratsExtLazyQueryHookResult = ReturnType<
  typeof useStratsExtLazyQuery
>
export type StratsExtQueryResult = Apollo.QueryResult<
  StratsExtQuery,
  StratsExtQueryVariables
>
export function refetchStratsExtQuery(variables?: StratsExtQueryVariables) {
  return { query: StratsExtDocument, variables: variables }
}
export const ChangeStratStateDocument = gql`
  mutation changeStratState($tktc: TypeAndKeyInput!, $fm: StateFieldMutation!) {
    state(tk: $tktc, fm: $fm)
  }
`
export type ChangeStratStateMutationFn = Apollo.MutationFunction<
  ChangeStratStateMutation,
  ChangeStratStateMutationVariables
>

/**
 * __useChangeStratStateMutation__
 *
 * To run a mutation, you first call `useChangeStratStateMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useChangeStratStateMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [changeStratStateMutation, { data, loading, error }] = useChangeStratStateMutation({
 *   variables: {
 *      tktc: // value for 'tktc'
 *      fm: // value for 'fm'
 *   },
 * });
 */
export function useChangeStratStateMutation(
  baseOptions?: Apollo.MutationHookOptions<
    ChangeStratStateMutation,
    ChangeStratStateMutationVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useMutation<
    ChangeStratStateMutation,
    ChangeStratStateMutationVariables
  >(ChangeStratStateDocument, options)
}
export type ChangeStratStateMutationHookResult = ReturnType<
  typeof useChangeStratStateMutation
>
export type ChangeStratStateMutationResult =
  Apollo.MutationResult<ChangeStratStateMutation>
export type ChangeStratStateMutationOptions = Apollo.BaseMutationOptions<
  ChangeStratStateMutation,
  ChangeStratStateMutationVariables
>
export const CancelDocument = gql`
  mutation cancel($tktc: TypeAndKeyInput!) {
    cancelOngoingOp(tk: $tktc)
  }
`
export type CancelMutationFn = Apollo.MutationFunction<
  CancelMutation,
  CancelMutationVariables
>

/**
 * __useCancelMutation__
 *
 * To run a mutation, you first call `useCancelMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCancelMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [cancelMutation, { data, loading, error }] = useCancelMutation({
 *   variables: {
 *      tktc: // value for 'tktc'
 *   },
 * });
 */
export function useCancelMutation(
  baseOptions?: Apollo.MutationHookOptions<
    CancelMutation,
    CancelMutationVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useMutation<CancelMutation, CancelMutationVariables>(
    CancelDocument,
    options
  )
}
export type CancelMutationHookResult = ReturnType<typeof useCancelMutation>
export type CancelMutationResult = Apollo.MutationResult<CancelMutation>
export type CancelMutationOptions = Apollo.BaseMutationOptions<
  CancelMutation,
  CancelMutationVariables
>
export const ResetModelDocument = gql`
  mutation resetModel($tktc: TypeAndKeyInput!, $mr: ModelReset!) {
    resetModel(tk: $tktc, mr: $mr)
  }
`
export type ResetModelMutationFn = Apollo.MutationFunction<
  ResetModelMutation,
  ResetModelMutationVariables
>

/**
 * __useResetModelMutation__
 *
 * To run a mutation, you first call `useResetModelMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useResetModelMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [resetModelMutation, { data, loading, error }] = useResetModelMutation({
 *   variables: {
 *      tktc: // value for 'tktc'
 *      mr: // value for 'mr'
 *   },
 * });
 */
export function useResetModelMutation(
  baseOptions?: Apollo.MutationHookOptions<
    ResetModelMutation,
    ResetModelMutationVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useMutation<ResetModelMutation, ResetModelMutationVariables>(
    ResetModelDocument,
    options
  )
}
export type ResetModelMutationHookResult = ReturnType<
  typeof useResetModelMutation
>
export type ResetModelMutationResult = Apollo.MutationResult<ResetModelMutation>
export type ResetModelMutationOptions = Apollo.BaseMutationOptions<
  ResetModelMutation,
  ResetModelMutationVariables
>
export const LifecycleCmdDocument = gql`
  mutation lifecycleCmd($tktc: TypeAndKeyInput!, $lc: StrategyLifecycleCmd!) {
    lifecycleCmd(tk: $tktc, slc: $lc)
  }
`
export type LifecycleCmdMutationFn = Apollo.MutationFunction<
  LifecycleCmdMutation,
  LifecycleCmdMutationVariables
>

/**
 * __useLifecycleCmdMutation__
 *
 * To run a mutation, you first call `useLifecycleCmdMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useLifecycleCmdMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [lifecycleCmdMutation, { data, loading, error }] = useLifecycleCmdMutation({
 *   variables: {
 *      tktc: // value for 'tktc'
 *      lc: // value for 'lc'
 *   },
 * });
 */
export function useLifecycleCmdMutation(
  baseOptions?: Apollo.MutationHookOptions<
    LifecycleCmdMutation,
    LifecycleCmdMutationVariables
  >
) {
  const options = { ...defaultOptions, ...baseOptions }
  return Apollo.useMutation<
    LifecycleCmdMutation,
    LifecycleCmdMutationVariables
  >(LifecycleCmdDocument, options)
}
export type LifecycleCmdMutationHookResult = ReturnType<
  typeof useLifecycleCmdMutation
>
export type LifecycleCmdMutationResult =
  Apollo.MutationResult<LifecycleCmdMutation>
export type LifecycleCmdMutationOptions = Apollo.BaseMutationOptions<
  LifecycleCmdMutation,
  LifecycleCmdMutationVariables
>

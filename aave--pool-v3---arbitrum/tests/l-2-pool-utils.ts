import { newMockEvent } from "matchstick-as"
import { ethereum, Address, BigInt } from "@graphprotocol/graph-ts"
import {
  BackUnbacked,
  Borrow,
  FlashLoan,
  IsolationModeTotalDebtUpdated,
  LiquidationCall,
  MintUnbacked,
  MintedToTreasury,
  RebalanceStableBorrowRate,
  Repay,
  ReserveDataUpdated,
  ReserveUsedAsCollateralDisabled,
  ReserveUsedAsCollateralEnabled,
  Supply,
  SwapBorrowRateMode,
  UserEModeSet,
  Withdraw
} from "../generated/L2Pool/L2Pool"

export function createBackUnbackedEvent(
  reserve: Address,
  backer: Address,
  amount: BigInt,
  fee: BigInt
): BackUnbacked {
  let backUnbackedEvent = changetype<BackUnbacked>(newMockEvent())

  backUnbackedEvent.parameters = new Array()

  backUnbackedEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  backUnbackedEvent.parameters.push(
    new ethereum.EventParam("backer", ethereum.Value.fromAddress(backer))
  )
  backUnbackedEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  backUnbackedEvent.parameters.push(
    new ethereum.EventParam("fee", ethereum.Value.fromUnsignedBigInt(fee))
  )

  return backUnbackedEvent
}

export function createBorrowEvent(
  reserve: Address,
  user: Address,
  onBehalfOf: Address,
  amount: BigInt,
  interestRateMode: i32,
  borrowRate: BigInt,
  referralCode: i32
): Borrow {
  let borrowEvent = changetype<Borrow>(newMockEvent())

  borrowEvent.parameters = new Array()

  borrowEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam(
      "onBehalfOf",
      ethereum.Value.fromAddress(onBehalfOf)
    )
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam(
      "interestRateMode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(interestRateMode))
    )
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam(
      "borrowRate",
      ethereum.Value.fromUnsignedBigInt(borrowRate)
    )
  )
  borrowEvent.parameters.push(
    new ethereum.EventParam(
      "referralCode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(referralCode))
    )
  )

  return borrowEvent
}

export function createFlashLoanEvent(
  target: Address,
  initiator: Address,
  asset: Address,
  amount: BigInt,
  interestRateMode: i32,
  premium: BigInt,
  referralCode: i32
): FlashLoan {
  let flashLoanEvent = changetype<FlashLoan>(newMockEvent())

  flashLoanEvent.parameters = new Array()

  flashLoanEvent.parameters.push(
    new ethereum.EventParam("target", ethereum.Value.fromAddress(target))
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam("initiator", ethereum.Value.fromAddress(initiator))
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam("asset", ethereum.Value.fromAddress(asset))
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam(
      "interestRateMode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(interestRateMode))
    )
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam(
      "premium",
      ethereum.Value.fromUnsignedBigInt(premium)
    )
  )
  flashLoanEvent.parameters.push(
    new ethereum.EventParam(
      "referralCode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(referralCode))
    )
  )

  return flashLoanEvent
}

export function createIsolationModeTotalDebtUpdatedEvent(
  asset: Address,
  totalDebt: BigInt
): IsolationModeTotalDebtUpdated {
  let isolationModeTotalDebtUpdatedEvent =
    changetype<IsolationModeTotalDebtUpdated>(newMockEvent())

  isolationModeTotalDebtUpdatedEvent.parameters = new Array()

  isolationModeTotalDebtUpdatedEvent.parameters.push(
    new ethereum.EventParam("asset", ethereum.Value.fromAddress(asset))
  )
  isolationModeTotalDebtUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "totalDebt",
      ethereum.Value.fromUnsignedBigInt(totalDebt)
    )
  )

  return isolationModeTotalDebtUpdatedEvent
}

export function createLiquidationCallEvent(
  collateralAsset: Address,
  debtAsset: Address,
  user: Address,
  debtToCover: BigInt,
  liquidatedCollateralAmount: BigInt,
  liquidator: Address,
  receiveAToken: boolean
): LiquidationCall {
  let liquidationCallEvent = changetype<LiquidationCall>(newMockEvent())

  liquidationCallEvent.parameters = new Array()

  liquidationCallEvent.parameters.push(
    new ethereum.EventParam(
      "collateralAsset",
      ethereum.Value.fromAddress(collateralAsset)
    )
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam("debtAsset", ethereum.Value.fromAddress(debtAsset))
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam(
      "debtToCover",
      ethereum.Value.fromUnsignedBigInt(debtToCover)
    )
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam(
      "liquidatedCollateralAmount",
      ethereum.Value.fromUnsignedBigInt(liquidatedCollateralAmount)
    )
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam(
      "liquidator",
      ethereum.Value.fromAddress(liquidator)
    )
  )
  liquidationCallEvent.parameters.push(
    new ethereum.EventParam(
      "receiveAToken",
      ethereum.Value.fromBoolean(receiveAToken)
    )
  )

  return liquidationCallEvent
}

export function createMintUnbackedEvent(
  reserve: Address,
  user: Address,
  onBehalfOf: Address,
  amount: BigInt,
  referralCode: i32
): MintUnbacked {
  let mintUnbackedEvent = changetype<MintUnbacked>(newMockEvent())

  mintUnbackedEvent.parameters = new Array()

  mintUnbackedEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  mintUnbackedEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  mintUnbackedEvent.parameters.push(
    new ethereum.EventParam(
      "onBehalfOf",
      ethereum.Value.fromAddress(onBehalfOf)
    )
  )
  mintUnbackedEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  mintUnbackedEvent.parameters.push(
    new ethereum.EventParam(
      "referralCode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(referralCode))
    )
  )

  return mintUnbackedEvent
}

export function createMintedToTreasuryEvent(
  reserve: Address,
  amountMinted: BigInt
): MintedToTreasury {
  let mintedToTreasuryEvent = changetype<MintedToTreasury>(newMockEvent())

  mintedToTreasuryEvent.parameters = new Array()

  mintedToTreasuryEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  mintedToTreasuryEvent.parameters.push(
    new ethereum.EventParam(
      "amountMinted",
      ethereum.Value.fromUnsignedBigInt(amountMinted)
    )
  )

  return mintedToTreasuryEvent
}

export function createRebalanceStableBorrowRateEvent(
  reserve: Address,
  user: Address
): RebalanceStableBorrowRate {
  let rebalanceStableBorrowRateEvent = changetype<RebalanceStableBorrowRate>(
    newMockEvent()
  )

  rebalanceStableBorrowRateEvent.parameters = new Array()

  rebalanceStableBorrowRateEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  rebalanceStableBorrowRateEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )

  return rebalanceStableBorrowRateEvent
}

export function createRepayEvent(
  reserve: Address,
  user: Address,
  repayer: Address,
  amount: BigInt,
  useATokens: boolean
): Repay {
  let repayEvent = changetype<Repay>(newMockEvent())

  repayEvent.parameters = new Array()

  repayEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  repayEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  repayEvent.parameters.push(
    new ethereum.EventParam("repayer", ethereum.Value.fromAddress(repayer))
  )
  repayEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  repayEvent.parameters.push(
    new ethereum.EventParam(
      "useATokens",
      ethereum.Value.fromBoolean(useATokens)
    )
  )

  return repayEvent
}

export function createReserveDataUpdatedEvent(
  reserve: Address,
  liquidityRate: BigInt,
  stableBorrowRate: BigInt,
  variableBorrowRate: BigInt,
  liquidityIndex: BigInt,
  variableBorrowIndex: BigInt
): ReserveDataUpdated {
  let reserveDataUpdatedEvent = changetype<ReserveDataUpdated>(newMockEvent())

  reserveDataUpdatedEvent.parameters = new Array()

  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "liquidityRate",
      ethereum.Value.fromUnsignedBigInt(liquidityRate)
    )
  )
  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "stableBorrowRate",
      ethereum.Value.fromUnsignedBigInt(stableBorrowRate)
    )
  )
  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "variableBorrowRate",
      ethereum.Value.fromUnsignedBigInt(variableBorrowRate)
    )
  )
  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "liquidityIndex",
      ethereum.Value.fromUnsignedBigInt(liquidityIndex)
    )
  )
  reserveDataUpdatedEvent.parameters.push(
    new ethereum.EventParam(
      "variableBorrowIndex",
      ethereum.Value.fromUnsignedBigInt(variableBorrowIndex)
    )
  )

  return reserveDataUpdatedEvent
}

export function createReserveUsedAsCollateralDisabledEvent(
  reserve: Address,
  user: Address
): ReserveUsedAsCollateralDisabled {
  let reserveUsedAsCollateralDisabledEvent =
    changetype<ReserveUsedAsCollateralDisabled>(newMockEvent())

  reserveUsedAsCollateralDisabledEvent.parameters = new Array()

  reserveUsedAsCollateralDisabledEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  reserveUsedAsCollateralDisabledEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )

  return reserveUsedAsCollateralDisabledEvent
}

export function createReserveUsedAsCollateralEnabledEvent(
  reserve: Address,
  user: Address
): ReserveUsedAsCollateralEnabled {
  let reserveUsedAsCollateralEnabledEvent =
    changetype<ReserveUsedAsCollateralEnabled>(newMockEvent())

  reserveUsedAsCollateralEnabledEvent.parameters = new Array()

  reserveUsedAsCollateralEnabledEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  reserveUsedAsCollateralEnabledEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )

  return reserveUsedAsCollateralEnabledEvent
}

export function createSupplyEvent(
  reserve: Address,
  user: Address,
  onBehalfOf: Address,
  amount: BigInt,
  referralCode: i32
): Supply {
  let supplyEvent = changetype<Supply>(newMockEvent())

  supplyEvent.parameters = new Array()

  supplyEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  supplyEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  supplyEvent.parameters.push(
    new ethereum.EventParam(
      "onBehalfOf",
      ethereum.Value.fromAddress(onBehalfOf)
    )
  )
  supplyEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )
  supplyEvent.parameters.push(
    new ethereum.EventParam(
      "referralCode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(referralCode))
    )
  )

  return supplyEvent
}

export function createSwapBorrowRateModeEvent(
  reserve: Address,
  user: Address,
  interestRateMode: i32
): SwapBorrowRateMode {
  let swapBorrowRateModeEvent = changetype<SwapBorrowRateMode>(newMockEvent())

  swapBorrowRateModeEvent.parameters = new Array()

  swapBorrowRateModeEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  swapBorrowRateModeEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  swapBorrowRateModeEvent.parameters.push(
    new ethereum.EventParam(
      "interestRateMode",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(interestRateMode))
    )
  )

  return swapBorrowRateModeEvent
}

export function createUserEModeSetEvent(
  user: Address,
  categoryId: i32
): UserEModeSet {
  let userEModeSetEvent = changetype<UserEModeSet>(newMockEvent())

  userEModeSetEvent.parameters = new Array()

  userEModeSetEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  userEModeSetEvent.parameters.push(
    new ethereum.EventParam(
      "categoryId",
      ethereum.Value.fromUnsignedBigInt(BigInt.fromI32(categoryId))
    )
  )

  return userEModeSetEvent
}

export function createWithdrawEvent(
  reserve: Address,
  user: Address,
  to: Address,
  amount: BigInt
): Withdraw {
  let withdrawEvent = changetype<Withdraw>(newMockEvent())

  withdrawEvent.parameters = new Array()

  withdrawEvent.parameters.push(
    new ethereum.EventParam("reserve", ethereum.Value.fromAddress(reserve))
  )
  withdrawEvent.parameters.push(
    new ethereum.EventParam("user", ethereum.Value.fromAddress(user))
  )
  withdrawEvent.parameters.push(
    new ethereum.EventParam("to", ethereum.Value.fromAddress(to))
  )
  withdrawEvent.parameters.push(
    new ethereum.EventParam("amount", ethereum.Value.fromUnsignedBigInt(amount))
  )

  return withdrawEvent
}

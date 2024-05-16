import {
  assert,
  describe,
  test,
  clearStore,
  beforeAll,
  afterAll
} from "matchstick-as/assembly/index"
import { Address, BigInt } from "@graphprotocol/graph-ts"
import { BackUnbacked } from "../generated/schema"
import { BackUnbacked as BackUnbackedEvent } from "../generated/L2Pool/L2Pool"
import { handleBackUnbacked } from "../src/l-2-pool"
import { createBackUnbackedEvent } from "./l-2-pool-utils"

// Tests structure (matchstick-as >=0.5.0)
// https://thegraph.com/docs/en/developer/matchstick/#tests-structure-0-5-0

describe("Describe entity assertions", () => {
  beforeAll(() => {
    let reserve = Address.fromString(
      "0x0000000000000000000000000000000000000001"
    )
    let backer = Address.fromString(
      "0x0000000000000000000000000000000000000001"
    )
    let amount = BigInt.fromI32(234)
    let fee = BigInt.fromI32(234)
    let newBackUnbackedEvent = createBackUnbackedEvent(
      reserve,
      backer,
      amount,
      fee
    )
    handleBackUnbacked(newBackUnbackedEvent)
  })

  afterAll(() => {
    clearStore()
  })

  // For more test scenarios, see:
  // https://thegraph.com/docs/en/developer/matchstick/#write-a-unit-test

  test("BackUnbacked created and stored", () => {
    assert.entityCount("BackUnbacked", 1)

    // 0xa16081f360e3847006db660bae1c6d1b2e17ec2a is the default address used in newMockEvent() function
    assert.fieldEquals(
      "BackUnbacked",
      "0xa16081f360e3847006db660bae1c6d1b2e17ec2a-1",
      "reserve",
      "0x0000000000000000000000000000000000000001"
    )
    assert.fieldEquals(
      "BackUnbacked",
      "0xa16081f360e3847006db660bae1c6d1b2e17ec2a-1",
      "backer",
      "0x0000000000000000000000000000000000000001"
    )
    assert.fieldEquals(
      "BackUnbacked",
      "0xa16081f360e3847006db660bae1c6d1b2e17ec2a-1",
      "amount",
      "234"
    )
    assert.fieldEquals(
      "BackUnbacked",
      "0xa16081f360e3847006db660bae1c6d1b2e17ec2a-1",
      "fee",
      "234"
    )

    // More assert options:
    // https://thegraph.com/docs/en/developer/matchstick/#asserts
  })
})

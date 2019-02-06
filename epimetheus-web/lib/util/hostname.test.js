import { expandHostname } from './hostname'

test('do nothing with non-match inputs', () => {
  expect(expandHostname(['example.com', 'foo123.com'])).toEqual([
    'example.com',
    'foo123.com',
  ])
})

test('expand single brace', () => {
  expect(expandHostname(['[1-3]'])).toEqual(['1', '2', '3'])
})

test('expand multiple braces', () => {
  expect(expandHostname(['[1-3][4-5]'])).toEqual([
    '14',
    '15',
    '24',
    '25',
    '34',
    '35',
  ])
})

test('pad zero', () => {
  expect(expandHostname(['[01-03]'])).toEqual(['01', '02', '03'])
})
